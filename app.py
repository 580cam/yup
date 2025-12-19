from flask import Flask, render_template, request, jsonify, send_file
import requests
import json
import csv
import io
import time
import re
import uuid
import os
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests
from apify_client import ApifyClient
from openai import OpenAI

app = Flask(__name__)

# Apify API credentials from environment
APIFY_API_TOKEN = os.environ.get('APIFY_API_TOKEN', '')
apify_client = ApifyClient(APIFY_API_TOKEN) if APIFY_API_TOKEN else None

# OpenAI API credentials from environment
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY', '')
openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# Rotating user agents - look more real (30+ different browsers)
USER_AGENTS = [
    # Chrome Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    # Chrome Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    # Firefox Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
    'Mozilla/5.0 (Windows NT 11.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
    # Firefox Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0',
    # Safari Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15',
    # Edge Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
    # Brave
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Brave/131.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Brave/131.0.0.0',
    # Different screen resolutions implied
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]

import random

def get_random_ua():
    return random.choice(USER_AGENTS)

# ProxyJet credentials
PROXY_USERNAME_BASE = "251216vin3B-resi-US"
PROXY_PASSWORD = "Ib4MCO7Q8YIsylv"
PROXY_SERVER = "proxy-jet.io:1010"

def get_sticky_proxy_url(session_id):
    """Get sticky proxy URL - same IP for entire journey"""
    username = f"{PROXY_USERNAME_BASE}-ip-{session_id}"
    return f"http://{username}:{PROXY_PASSWORD}@{PROXY_SERVER}"

# Chrome 131 headers - matches TLS impersonation EXACTLY
CHROME_131_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'sec-ch-ua': '"Chromium";v="131", "Not_A Brand";v="24", "Google Chrome";v="131"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1'
}

print(f"Using ProxyJet rotating proxies with sticky sessions")

# Safe int conversion for CSV fields that might be empty strings
def safe_int(value, default=0):
    """Convert value to int, handling empty strings and None"""
    if not value or value == '':
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def search_social_media_with_apify(agent_name, city_state, platform):
    """
    Use Apify to search for missing social media profiles
    platform: 'instagram' or 'facebook'
    """
    try:
        # Build search query
        site = f"site:{platform}.com" if platform in ['instagram', 'facebook'] else platform
        query = f'{site} "{agent_name}" realtor {city_state.replace("-", " ")}'

        print(f"  Query: {query}")

        # Apify Google Search actor input
        run_input = {
            "queries": query,
            "resultsPerPage": 10,
            "maxPagesPerQuery": 1,
            "aiMode": "aiModeOff",
            "saveHtml": False,
            "saveHtmlToKeyValueStore": False,
        }

        print(f"  Calling Apify actor...")
        # Run the Actor and wait for it to finish
        run = apify_client.actor("nFJndFXA5zjCTuudP").call(run_input=run_input)
        print(f"  Apify run completed. Dataset ID: {run['defaultDatasetId']}")

        # Fetch results
        results = []
        for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
            if 'organicResults' in item:
                for result in item['organicResults']:
                    url = result.get('url', '')

                    # Filter for PROFILES ONLY (not posts/reels/stories)
                    if platform == 'instagram':
                        # Instagram profiles: instagram.com/username/ or instagram.com/username
                        # Exclude posts (/p/), reels (/reel/), stories (/stories/), etc.
                        if '/p/' in url or '/reel/' in url or '/stories/' in url or '/tv/' in url or '/explore/' in url:
                            continue  # Skip posts/reels/stories
                    elif platform == 'facebook':
                        # Facebook profiles: facebook.com/username or facebook.com/pages/...
                        # Exclude posts, photos, events, groups
                        if any(x in url for x in ['/posts/', '/photo.php', '/permalink.php', '/story.php', '/events/', '/groups/', '/videos/']):
                            continue  # Skip posts/photos/events/groups

                    results.append({
                        'title': result.get('title', ''),
                        'url': url,
                        'description': result.get('description', '')
                    })

        print(f"  ✓ Found {len(results)} {platform} profile results from Apify")
        if results:
            for i, r in enumerate(results[:3], 1):
                print(f"    {i}. {r['title'][:60]}...")

        return results

    except Exception as e:
        print(f"  ❌ Error searching {platform}: {e}")
        import traceback
        traceback.print_exc()
        return []

def match_social_profile_with_ai(agent_data, search_results, platform):
    """
    Use OpenAI GPT-4o-mini to match search results with agent profile
    Returns the most likely social media URL or None
    """
    if not search_results:
        return None

    try:
        # Build context from search results
        search_context = "\n\n".join([
            f"Result {i+1}:\n- Title: {r.get('title', '')}\n- URL: {r.get('url', '')}\n- Description: {r.get('description', '')}"
            for i, r in enumerate(search_results)
        ])

        # Build agent context
        agent_context = f"""Agent Information:
- Name: {agent_data.get('firstName', '')} {agent_data.get('lastName', '')}
- Location: {agent_data.get('city', '')}
- Profession: Real Estate Agent"""

        # AI prompt - VERY STRICT matching
        prompt = f"""{agent_context}

Search Results from Google:
{search_context}

Task: Analyze these {platform} search results and determine which profile belongs to this EXACT real estate agent.

STRICT Matching Criteria (ALL must be met):
1. **Name match**: First name AND last name must match EXACTLY or be very close variations (e.g., "Chris" vs "Christopher")
2. **Location match**: Must be in the same metro area or nearby cities (Oklahoma City area includes: Oklahoma City, Edmond, Norman, Moore, Yukon, Mustang, etc.)
3. **Profession**: Profile must clearly indicate real estate agent/realtor/broker or show a real estate company (Keller Williams, RE/MAX, Coldwell Banker, etc.)
4. **Profile type**: Must be a PROFILE, not a post or random mention

MATCHING GUIDELINES:
- Same first + last name + real estate profession + same metro area = MATCH
- If multiple results show the same person repeatedly = high confidence match
- Different state or far away city = NO MATCH
- Different profession or no real estate indicators = NO MATCH
- Generic profile with no location/profession info = NO MATCH

IMPORTANT: Only return a match if you're 90%+ confident it's the EXACT SAME PERSON. If unclear, return "null".

Response format: Return ONLY the full URL of the match, or "null" if not confident. No explanation, just the URL or null."""

        # Call OpenAI with strict but reasonable matching
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a data matching assistant for real estate agents. Match profiles when name + profession + location align well. If multiple results show the same person, that's high confidence. Return null only if unclear or different person."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,  # Strict but not overly rigid
            max_tokens=200
        )

        result = response.choices[0].message.content.strip()

        if result.lower() == "null" or not result.startswith("http"):
            print(f"  ✗ AI found no confident {platform} match")
            return None

        print(f"  ✓ AI matched {platform}: {result}")
        return result

    except Exception as e:
        print(f"  Error in AI matching for {platform}: {e}")
        return None

GRAPHQL_URL = "https://www.realtor.com/frontdoor/graphql"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Origin': 'https://www.realtor.com',
    'Referer': 'https://www.realtor.com/realestateagents',
    'rdc-client-name': 'RDC_WEB',
    'rdc-client-version': 'island-agent-branding-75febb3b',
    'rdc-app-id': 'rdc-web',
    'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="143"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin'
}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/test')
def test():
    return jsonify({'status': 'working', 'message': 'API is alive - GraphQL version'})

@app.route('/api/scrape', methods=['POST'])
def scrape():
    print("=" * 80)
    print("SCRAPE REQUEST RECEIVED!")
    print("=" * 80)

    data = request.json
    print(f"Request data: {data}")

    mode = data.get('mode')
    print(f"Mode: {mode}")

    def generate():
        """Stream results immediately - no batching"""
        if mode == 'area':
            areas = data.get('areas', [])
            for area in areas:
                enriched_count = 0
                total_agents = 0

                # Stream ALL agents (no limit!)
                for agent in stream_agents_from_area(area):
                    if agent is None:
                        yield ": keepalive\n\n"
                        continue

                    total_agents += 1
                    enriched_count += 1

                    try:
                        print(f"========================================")
                        print(f"ENRICHING AGENT #{enriched_count}")
                        print(f"========================================")

                        # Send multiple keepalives to prevent timeout during long enrichment
                        for _ in range(3):
                            yield ": keepalive\n\n"

                        # Enrich ALL agents with Zillow
                        enriched = enrich_realtor(agent)

                        # Yield immediately!
                        json_str = json.dumps(enriched, ensure_ascii=True, separators=(',', ':'))
                        yield f"data: {json_str}\n\n"

                    except Exception as e:
                        print(f"Error encoding agent: {e}")
                        continue

        elif mode == 'csv':
            leads = data.get('leads', [])
            print(f"Processing {len(leads)} leads from CSV for Zillow enrichment...")

            for i, lead in enumerate(leads):
                if not lead.get('firstName') or not lead.get('lastName'):
                    continue

                try:
                    print(f"")
                    print(f"========================================")
                    print(f"ENRICHING CSV LEAD {i+1}/{len(leads)}")
                    print(f"========================================")

                    # Send multiple keepalives to prevent timeout during long enrichment
                    for _ in range(3):
                        yield ": keepalive\n\n"

                    # CSV mode: ONLY Zillow enrichment (they already have realtor.com data)
                    enriched = enrich_csv_lead_with_zillow(lead)

                    json_str = json.dumps(enriched, ensure_ascii=True, separators=(',', ':'))
                    yield f"data: {json_str}\n\n"

                except Exception as e:
                    print(f"Error encoding lead: {e}")
                    continue

        yield 'data: {"done":true}\n\n'

    return app.response_class(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no',
        'Connection': 'keep-alive',
        'Keep-Alive': 'timeout=86400'
    })

def stream_agents_from_area(area):
    """Generator that yields agents one at a time as they're fetched"""
    try:
        # Skip homepage - go directly to pages we need

        # First, search for the location
        location_query = {
            "operationName": "AgentLocationSearch",
            "variables": {
                "locationSearchInput": {
                    "input": area,
                    "client_id": "agent-branding-search",
                    "limit": 10,
                    "area_types": "city,postal_code"
                }
            },
            "query": """query AgentLocationSearch($locationSearchInput: AgentLocationSearchInput) {
                agents_location_search(location_search_input: $locationSearchInput) {
                    auto_complete {
                        id
                        slug_id
                        city
                        state_code
                        postal_code
                        __typename
                    }
                    __typename
                }
            }"""
        }

        # Retry location search up to 3 times if it fails
        location_data = None
        for attempt in range(3):
            response = requests.post(GRAPHQL_URL, json=location_query, headers=HEADERS, timeout=15)

            print(f"Location search attempt {attempt + 1}: status {response.status_code}")

            if response.status_code != 200:
                print(f"Location search failed with status {response.status_code}, retrying...")
                time.sleep(2)
                continue

            try:
                location_data = response.json()
                if 'errors' in location_data:
                    print(f"Location search errors: {location_data['errors']}, retrying...")
                    time.sleep(2)
                    continue
                # Success!
                break
            except Exception as e:
                print(f"Failed to parse location data JSON: {e}, retrying...")
                time.sleep(2)
                continue

        if not location_data:
            print("Location search failed after 3 attempts")
            return

        if location_data and 'data' in location_data and location_data['data'] and 'agents_location_search' in location_data['data']:
            locations = location_data['data']['agents_location_search']['auto_complete']

            if locations:
                location = locations[0]
                slug_id = location.get('slug_id')

                print(f"Found location: {slug_id}")

                # Stream agents from location
                for agent in stream_agents_in_location(slug_id):
                    yield agent

    except Exception as e:
        print(f"Error finding agents via GraphQL: {e}")
        import traceback
        traceback.print_exc()

def stream_agents_in_location(slug_id):
    """Generator that yields agents one at a time - pagination through all pages"""
    try:
        # Convert slug_id format: "Oklahoma-City_OK" -> "ok_oklahoma-city"
        parts = slug_id.split('_')
        if len(parts) == 2:
            city_part = parts[0].lower()
            state_part = parts[1].lower()
            marketing_area = f"{state_part}_{city_part}"
        else:
            marketing_area = slug_id.lower()

        print(f"Using marketing_area_city: {marketing_area}")

        offset = 0
        limit = 50
        total_rows = None
        seen_agent_ids = set()  # Track unique agents to prevent duplicates

        # Paginate through all results
        while True:
            agents_query = {
                "operationName": "SearchAgents",
                "variables": {
                    "searchAgentInput": {
                        "name": "",
                        "postal_code": "",
                        "languages": [],
                        "agent_type": None,
                        "marketing_area_city": marketing_area,
                        "sort": "RELEVANT_AGENTS",
                        "offset": offset,
                        "agent_filter_criteria": "NRDS_AND_FULFILLMENT_ID_EXISTS",
                        "limit": limit
                    }
                },
                "query": """query SearchAgents($searchAgentInput: SearchAgentInput) {
                    search_agents(search_agent_input: $searchAgentInput) {
                        agents {
                            id
                            fulfillment_id
                            fullname
                            listing_stats {
                                combined_annual {
                                    min
                                    max
                                    __typename
                                }
                                for_sale {
                                    count
                                    last_listing_date
                                    __typename
                                }
                                recently_sold_annual {
                                    count
                                    __typename
                                }
                                recently_sold_listing_details {
                                    listings {
                                        baths
                                        beds
                                        city
                                        state_code
                                        __typename
                                    }
                                    __typename
                                }
                                __typename
                            }
                            ratings_reviews {
                                average_rating
                                recommendations_count
                                reviews_count
                                __typename
                            }
                            __typename
                        }
                        matching_rows
                        __typename
                    }
                }"""
            }

            response = requests.post(GRAPHQL_URL, json=agents_query, headers=HEADERS, timeout=15)
            data = response.json()

            if 'data' not in data or 'search_agents' not in data['data']:
                print("No data in response, stopping pagination")
                break

            search_data = data['data']['search_agents']
            agent_list = search_data.get('agents', [])
            matching_rows = search_data.get('matching_rows', 0)

            if total_rows is None:
                total_rows = matching_rows
                print(f"Total agents available: {total_rows}")

            print(f"Page offset {offset}: Got {len(agent_list)} agents")

            if not agent_list:
                print("No more agents, stopping pagination")
                break

            for agent in agent_list:
                # Build profile URL from agent ID (not fulfillment_id!)
                agent_id = agent.get('id', '')

                # Skip duplicates
                if agent_id in seen_agent_ids:
                    continue
                seen_agent_ids.add(agent_id)

                # Parse agent data
                full_name = agent.get('fullname', '')
                name_parts = full_name.split()

                first_name = name_parts[0] if name_parts else ''
                last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

                profile_url = f"https://www.realtor.com/realestateagents/{agent_id}" if agent_id else ''

                # Get sales stats
                stats = agent.get('listing_stats', {})
                sales_12mo = stats.get('recently_sold_annual', {}).get('count') or 0
                for_sale_count = stats.get('for_sale', {}).get('count') or 0

                # Get recent sales
                recent_sales_data = stats.get('recently_sold_listing_details', {}).get('listings', [])

                # Yield agent immediately instead of appending to list
                yield {
                    'firstName': first_name,
                    'lastName': last_name,
                    'profileUrl': profile_url,
                    'agentId': agent_id,
                    'sales12Months': sales_12mo,
                    'totalSales': for_sale_count,
                    'recentSales': recent_sales_data[:5],
                    'stats': stats,
                    'cityState': slug_id  # Pass city/state for Zillow search
                }

            # Move to next page
            offset += limit

            # Stop if we've fetched all available agents
            if offset >= total_rows:
                print(f"Fetched all {total_rows} agents, stopping")
                break

            # Send keepalive comment every page to prevent timeout
            yield None  # Will send a keepalive in the generate function

        print(f"Streaming complete")

    except Exception as e:
        print(f"Error searching agents: {e}")
        import traceback
        traceback.print_exc()

def init_zillow_session():
    """Initialize Zillow session by visiting homepage to get cookies"""
    try:
        # Visit Zillow homepage with ProxyJet rotating residential proxy
        proxies = {'http': PROXY_URL, 'https': PROXY_URL}

        print(f"Initializing Zillow session with proxy: {PROXY_SERVER}")
        response = zillow_session.get('https://www.zillow.com/', impersonate="chrome120", proxies=proxies, timeout=20)
        print(f"Zillow homepage status: {response.status_code}")
        print(f"Zillow session initialized - waiting before first request...")
    except Exception as e:
        print(f"Failed to init Zillow session: {e}")
        import traceback
        traceback.print_exc()

def enrich_with_zillow(first_name, last_name, city_state, realtor_12mo_sales):
    """Human Journey: Homepage → Search → Profile with sticky session"""

    max_retries = 2

    for attempt in range(max_retries):
        try:
            # Generate unique session ID for this agent (sticky IP)
            run_session_id = str(uuid.uuid4())[:8]
            sticky_proxy_url = get_sticky_proxy_url(run_session_id)
            proxies = {'http': sticky_proxy_url, 'https': sticky_proxy_url}

            # Random delay before starting (break patterns)
            time.sleep(random.uniform(2.0, 4.0))

            if attempt > 0:
                print(f"Retry #{attempt} for {first_name} {last_name} (new session: {run_session_id})")
            else:
                print(f"Starting Human Journey for {first_name} {last_name} (session: {run_session_id})")

            # Create FRESH session for this agent
            session = curl_requests.Session()

            # STEP 1: Visit homepage (lander) to get cookies
            print(f"  Step 1: Landing on homepage...")
            home_headers = CHROME_131_HEADERS.copy()
            home_headers['sec-fetch-site'] = 'none'  # Direct navigation

            home_response = session.get(
                'https://www.zillow.com/',
                headers=home_headers,
                impersonate="chrome131",
                proxies=proxies,
                timeout=20
            )

            if home_response.status_code != 200:
                session.close()
                raise Exception(f"Homepage returned {home_response.status_code}")
    
            print(f"  ✓ Homepage cookies collected")
            time.sleep(random.uniform(3.0, 5.0))  # Look like browsing
    
            # STEP 2: Search for agent
            full_name_realtor = f"{first_name} {last_name}".strip()
            from urllib.parse import quote_plus
            name_query = quote_plus(full_name_realtor)
            search_url = f"https://www.zillow.com/professionals/real-estate-agent-reviews/{city_state.lower().replace(' ', '-')}/?name={name_query}"
    
            print(f"  Step 2: Searching for {full_name_realtor}...")
    
            search_headers = CHROME_131_HEADERS.copy()
            search_headers['Referer'] = 'https://www.zillow.com/'
    
            search_response = session.get(
                search_url,
                headers=search_headers,
                impersonate="chrome131",
                proxies=proxies,
                timeout=20
            )
    
            if search_response.status_code != 200:
                session.close()
                raise Exception(f"Search returned {search_response.status_code}")
    
            print(f"  ✓ Search success! Status 200")
    
            soup = BeautifulSoup(search_response.content, 'html.parser')
    
            # Find __NEXT_DATA__ script tag with JSON
            script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
            if not script_tag:
                print("No __NEXT_DATA__ script found")
                return None
    
            data = json.loads(script_tag.string)
    
            # Navigate to profile data
            props = data.get('props', {}).get('pageProps', {}).get('displayData', {})
            results = props.get('agentFinderGraphData', {}).get('agentDirectoryFinderDisplay', {}).get('searchResults', {}).get('results', {}).get('resultsCards', [])
    
            print(f"Found {len(results)} Zillow results")
    
            # Clean name for matching - remove punctuation, only keep letters and spaces
            def clean_for_match(name):
                import string
                # Remove all punctuation except spaces
                translator = str.maketrans('', '', string.punctuation)
                cleaned = name.translate(translator)
                # Normalize multiple spaces to single space
                return ' '.join(cleaned.lower().split())
    
            # ONE LOOP: Collect both exact and fuzzy matches
            realtor_clean = clean_for_match(full_name_realtor)
            print(f"  Looking for: '{realtor_clean}'")
    
            exact_matches = []
            fuzzy_matches = []
    
            realtor_words = realtor_clean.split()
            realtor_first = realtor_words[0] if realtor_words else ''
            realtor_last = realtor_words[-1] if realtor_words else ''
    
            # Single loop through all results
            for card in results:
                if card.get('__typename') != 'AgentDirectoryFinderProfileResultsCard':
                    continue
    
                card_name = card.get('cardTitle', '')
                zillow_clean = clean_for_match(card_name)
    
                # Extract sales
                profile_data_temp = card.get('profileData', [])
                zillow_12mo = 0
                for item in profile_data_temp:
                    if 'sales last 12 months' in item.get('label', '').lower():
                        zillow_12mo = int(item.get('data') or 0)
                        break
    
                # Check EXACT match (bidirectional substring)
                if realtor_clean in zillow_clean or zillow_clean in realtor_clean:
                    print(f"  EXACT: '{card_name}' - {zillow_12mo} sales")
                    exact_matches.append({'card': card, 'name': card_name, 'zillow_12mo': zillow_12mo})
    
                # Also check FUZZY match (first 3 letters + last name)
                else:
                    zillow_words = zillow_clean.split()
                    zillow_first = zillow_words[0] if zillow_words else ''
                    zillow_last = zillow_words[-1] if zillow_words else ''
    
                    if (realtor_last == zillow_last and
                        len(realtor_first) >= 3 and len(zillow_first) >= 3 and
                        (realtor_first[:3] == zillow_first[:3])):
                        print(f"  FUZZY: '{card_name}' - {zillow_12mo} sales")
                        fuzzy_matches.append({'card': card, 'name': card_name, 'zillow_12mo': zillow_12mo})
    
            # DECISION TREE:
            # 1. Find exact match with CLOSEST sales (within 10)
            # 2. Find fuzzy match with CLOSEST sales (within 10)
            # 3. Use first exact match
            # 4. Give up

            best_match = None
            best_diff = float('inf')

            # Check exact matches - find the one with CLOSEST sales
            for match in exact_matches:
                diff = abs(match['zillow_12mo'] - realtor_12mo_sales)
                if diff <= 10 and diff < best_diff:
                    best_match = match
                    best_diff = diff

            if best_match:
                print(f"  ✓ Using EXACT match with verified sales: {best_match['name']} (diff={best_diff})")

            # If no good exact, try fuzzy - find the one with CLOSEST sales
            if not best_match:
                best_diff = float('inf')
                for match in fuzzy_matches:
                    diff = abs(match['zillow_12mo'] - realtor_12mo_sales)
                    if diff <= 10 and diff < best_diff:
                        best_match = match
                        best_diff = diff

                if best_match:
                    print(f"  ✓ Using FUZZY match with verified sales: {best_match['name']} (diff={best_diff})")
    
            # Fallback to first exact
            if not best_match and exact_matches:
                best_match = exact_matches[0]
                print(f"  Using first EXACT match (no sales match): {best_match['name']}")
    
            if not best_match and fuzzy_matches:
                best_match = fuzzy_matches[0]
                print(f"  Using first FUZZY match (no sales match): {best_match['name']}")
    
            if not best_match:
                print(f"No matches found for '{full_name_realtor}'")
                return None
    
            # Extract profile data from matched card
            matched_card = best_match['card']
            profile_data = matched_card.get('profileData', [])
            zillow_url = matched_card.get('cardActionLink', '')
    
            print(f"Zillow URL: {zillow_url}")
    
            # Parse sales data
            total_sales_in_city = 0
            sales_last_12mo = 0
    
            for item in profile_data:
                label = item.get('label', '').lower()
                data_val = item.get('data')
    
                if 'sales in' in label and data_val:
                    total_sales_in_city = int(data_val)
                    print(f"  Total sales: {total_sales_in_city}")
                elif 'sales last 12 months' in label and data_val:
                    sales_last_12mo = int(data_val)
                    print(f"  12mo sales: {sales_last_12mo}")
    
            # STEP 3: Visit profile page
            delay = random.uniform(8.0, 15.0)
            print(f"  Step 3: Waiting {delay:.1f}s before clicking profile...")
            time.sleep(delay)
    
            # Scrape profile using SAME session
            profile_data = scrape_zillow_profile_journey(session, zillow_url, search_url, proxies)
    
            # Close session
            session.close()
    
            if profile_data:
                result = {
                    **profile_data,  # Merge profile data first
                    'zillowUrl': zillow_url,  # Override with correct URL
                    'totalSalesInCity': total_sales_in_city,
                    'sales12Months': sales_last_12mo
                }
    
                # Check for missing social media and search with Apify + AI
                missing_instagram = not result.get('instagramUrl', '')
                missing_facebook = not result.get('facebookUrl', '')
    
                print(f"")
                print(f"========== SOCIAL MEDIA CHECK ==========")
                print(f"  Instagram: {'MISSING' if missing_instagram else 'FOUND'}")
                print(f"  Facebook: {'MISSING' if missing_facebook else 'FOUND'}")
    
                if missing_instagram or missing_facebook:
                    if not apify_client or not openai_client:
                        print(f"  ⚠ AI search disabled - missing API keys")
                    else:
                        agent_info = {
                            'firstName': first_name,
                            'lastName': last_name,
                            'city': city_state
                        }
    
                        # Search for Instagram if missing
                        if missing_instagram:
                            print(f"")
                            print(f"  🔍 Starting Instagram search...")
                            instagram_results = search_social_media_with_apify(
                                f"{first_name} {last_name}",
                                city_state,
                                "instagram"
                            )
                            if instagram_results:
                                print(f"  🤖 Sending {len(instagram_results)} results to AI for matching...")
                                matched_instagram = match_social_profile_with_ai(agent_info, instagram_results, "instagram")
                                if matched_instagram:
                                    result['instagramUrl'] = matched_instagram
                                    print(f"  ✅ Instagram added: {matched_instagram}")
                                else:
                                    print(f"  ❌ AI found no confident Instagram match")
                            else:
                                print(f"  ❌ No Instagram results from Apify")
    
                        # Search for Facebook if missing
                        if missing_facebook:
                            print(f"")
                            print(f"  🔍 Starting Facebook search...")
                            facebook_results = search_social_media_with_apify(
                                f"{first_name} {last_name}",
                                city_state,
                                "facebook"
                            )
                            if facebook_results:
                                print(f"  🤖 Sending {len(facebook_results)} results to AI for matching...")
                                matched_facebook = match_social_profile_with_ai(agent_info, facebook_results, "facebook")
                                if matched_facebook:
                                    result['facebookUrl'] = matched_facebook
                                    print(f"  ✅ Facebook added: {matched_facebook}")
                                else:
                                    print(f"  ❌ AI found no confident Facebook match")
                            else:
                                print(f"  ❌ No Facebook results from Apify")
    
                print(f"========================================")
                print(f"")
    
                return result
    
            return {
                'zillowUrl': zillow_url,
                'totalSalesInCity': total_sales_in_city,
                'sales12Months': sales_last_12mo
            }

        except Exception as e:
            error_msg = str(e)

            # Check if it's a retryable error (timeout, 403, 429, 500s)
            is_timeout = 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower()
            is_403 = '403' in error_msg
            is_429 = '429' in error_msg
            is_5xx = any(f'{code}' in error_msg for code in [500, 502, 503, 504])

            if is_timeout:
                print(f"  ⏱️ Timeout error on attempt {attempt + 1}/{max_retries}")
            elif is_403:
                print(f"  🚫 403 Forbidden on attempt {attempt + 1}/{max_retries}")
            elif is_429:
                print(f"  🚦 429 Rate Limited on attempt {attempt + 1}/{max_retries}")
            elif is_5xx:
                print(f"  ⚠️ Server error on attempt {attempt + 1}/{max_retries}")

            if is_timeout or is_403 or is_429 or is_5xx:
                if attempt < max_retries - 1:
                    print(f"  🔄 Retrying with new session and proxy...")
                    time.sleep(random.uniform(3.0, 6.0))  # Extra delay before retry
                    continue  # Retry with new session
                else:
                    print(f"  ❌ All retries exhausted, skipping agent")
                    return None
            else:
                # Non-retryable error, don't retry
                print(f"Error in Human Journey for {first_name} {last_name}: {e}")
                import traceback
                traceback.print_exc()
                return None

    # If we get here, all retries failed
    return None

def scrape_zillow_profile_journey(session, profile_url, search_url, proxies):
    """STEP 3: Visit profile page with same session"""
    try:
        print(f"  Visiting profile: {profile_url}")

        # Use SAME session with cookies from homepage + search
        profile_headers = CHROME_131_HEADERS.copy()
        profile_headers['Referer'] = search_url  # Came from search results!

        response = session.get(
            profile_url,
            headers=profile_headers,
            impersonate="chrome131",
            proxies=proxies,
            timeout=20
        )

        if response.status_code != 200:
            raise Exception(f"Profile returned {response.status_code}")

        print(f"  ✓ Profile success! Status 200")

        soup = BeautifulSoup(response.content, 'html.parser')

        # Parse social media links from HTML directly (JSON often missing them)
        # ONLY look within the agent profile section to avoid Zillow corporate links
        html_socials = {'facebook': '', 'linkedin': '', 'instagram': '', 'twitter': '', 'youtube': ''}

        # Find all social media links in HTML with rel="noreferrer" (agent's personal links)
        for link in soup.find_all('a', href=True, rel=True):
            # Skip if not a profile link (agent links have rel="noreferrer")
            if 'noreferrer' not in link.get('rel', []):
                continue

            href = link.get('href', '').lower()

            # Skip Zillow's own social media
            if 'zillow' in href:
                continue

            if 'facebook.com' in href or 'fb.com' in href:
                html_socials['facebook'] = link['href']
                print(f"  Found Facebook (HTML): {link['href']}")
            elif 'instagram.com' in href:
                html_socials['instagram'] = link['href']
                print(f"  Found Instagram (HTML): {link['href']}")
            elif 'linkedin.com' in href:
                html_socials['linkedin'] = link['href']
                print(f"  Found LinkedIn (HTML): {link['href']}")
            elif 'twitter.com' in href or 'x.com' in href:
                html_socials['twitter'] = link['href']
                print(f"  Found Twitter (HTML): {link['href']}")
            elif 'youtube.com' in href:
                html_socials['youtube'] = link['href']
                print(f"  Found YouTube (HTML): {link['href']}")

        print(f"  HTML parsed social links: FB={bool(html_socials['facebook'])}, IG={bool(html_socials['instagram'])}, LI={bool(html_socials['linkedin'])}")

        # Find __NEXT_DATA__ script
        script_tag = soup.find('script', {'id': '__NEXT_DATA__'})
        if not script_tag:
            print("  No __NEXT_DATA__ on profile")
            return None

        data = json.loads(script_tag.string)
        props = data.get('props', {}).get('pageProps', {})

        # Extract all the data
        display_user = props.get('displayUser', {})
        sales_stats = props.get('agentSalesStats', {})
        get_to_know = props.get('getToKnowMe', {})
        past_sales = props.get('pastSales', {})
        service_areas = props.get('serviceAreas', [])

        email = display_user.get('email', '')
        phone_numbers = display_user.get('phoneNumbers', {})
        cell_phone = phone_numbers.get('cell', '')
        brokerage_phone = phone_numbers.get('brokerage', '')
        business_address = display_user.get('businessAddress', {})
        full_address = f"{business_address.get('address1', '')}, {business_address.get('city', '')}, {business_address.get('state', '')} {business_address.get('postalCode', '')}"

        # Get review count
        ratings = display_user.get('ratings', {})
        review_count = ratings.get('count', 0)

        # Ultra aggressive string cleaning for JSON safety
        def clean_str(s):
            if not s:
                return ''
            # Convert to string first
            s = str(s)
            # Remove newlines and tabs FIRST
            s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
            # Remove HTML
            import re
            s = re.sub(r'<[^>]+>', '', s)
            # Keep ONLY: letters, numbers, spaces, @ . , - / ( )
            s = re.sub(r'[^a-zA-Z0-9\s@\.\,\-\/\(\)]', '', s)
            # Single space normalization
            return ' '.join(s.split())

        # Get service areas
        areas = [clean_str(area.get('text', '')) for area in service_areas]

        # Get latest sale
        latest_sale_address = ''
        latest_sale_date = ''
        past_sales_list = past_sales.get('past_sales', [])
        if past_sales_list and len(past_sales_list) > 0:
            latest = past_sales_list[0]
            latest_sale_address = clean_str(latest.get('street_address', ''))
            latest_sale_date = latest.get('sold_date', '')  # Don't clean date!

        # Get social media URLs (Zillow sometimes mislabels them!)
        facebook_url = get_to_know.get('facebookUrl', '')
        linkedin_url = get_to_know.get('linkedInUrl', '')
        instagram_url = get_to_know.get('instagramUrl', '')
        twitter_url = get_to_know.get('twitterUrl', '')
        youtube_url = get_to_know.get('youtubeUrl', '')

        # DEBUG: Show raw URLs from Zillow JSON
        print(f"  RAW Zillow JSON social URLs:")
        print(f"    facebookUrl: {facebook_url}")
        print(f"    linkedInUrl: {linkedin_url}")
        print(f"    instagramUrl: {instagram_url}")
        print(f"    twitterUrl: {twitter_url}")
        print(f"    youtubeUrl: {youtube_url}")

        # Fix Zillow's mislabeling - check if URLs match their field names
        all_socials = [
            ('facebook', facebook_url),
            ('linkedin', linkedin_url),
            ('instagram', instagram_url),
            ('twitter', twitter_url),
            ('youtube', youtube_url)
        ]

        # Merge HTML and JSON social links (prefer HTML as it's more complete)
        corrected_socials = {
            'facebook': html_socials['facebook'] or facebook_url,
            'linkedin': html_socials['linkedin'] or linkedin_url,
            'instagram': html_socials['instagram'] or instagram_url,
            'twitter': html_socials['twitter'] or twitter_url,
            'youtube': html_socials['youtube'] or youtube_url
        }

        # Remap if URLs from JSON are mislabeled
        for field_name, url in all_socials:
            if not url or html_socials.get(field_name):  # Skip if HTML already has it
                continue

            url_lower = url.lower()

            # Detect actual platform from URL
            if 'instagram.com' in url_lower:
                corrected_socials['instagram'] = url
                print(f"  Found Instagram (JSON): {url}")
            elif 'facebook.com' in url_lower or 'fb.com' in url_lower:
                corrected_socials['facebook'] = url
                print(f"  Found Facebook (JSON): {url}")
            elif 'linkedin.com' in url_lower:
                corrected_socials['linkedin'] = url
                print(f"  Found LinkedIn (JSON): {url}")
            elif 'twitter.com' in url_lower or 'x.com' in url_lower:
                corrected_socials['twitter'] = url
                print(f"  Found Twitter (JSON): {url}")
            elif 'youtube.com' in url_lower:
                corrected_socials['youtube'] = url
                print(f"  Found YouTube (JSON): {url}")

        result = {
            'email': clean_str(email),
            'phone': clean_str(cell_phone),
            'brokeragePhone': clean_str(brokerage_phone),
            'totalSalesAllTime': sales_stats.get('countAllTime', 0),
            'yearsExperience': get_to_know.get('yearsInIndustry', 0),
            'reviewCount': review_count,
            'title': clean_str(get_to_know.get('title', '')),
            'description': clean_str(get_to_know.get('description', '')),
            'specialties': [clean_str(s) for s in get_to_know.get('specialties', [])],
            'serviceAreas': areas,
            'businessName': clean_str(display_user.get('businessName', '')),
            'businessAddress': clean_str(full_address.strip(', ')),
            'pronouns': clean_str(display_user.get('cpdUserPronouns', '')),
            'websiteUrl': get_to_know.get('websiteUrl', ''),
            'facebookUrl': corrected_socials['facebook'],
            'linkedInUrl': corrected_socials['linkedin'],
            'instagramUrl': corrected_socials['instagram'],
            'twitterUrl': corrected_socials['twitter'],
            'youtubeUrl': corrected_socials['youtube'],
            'latestSaleAddress': latest_sale_address,
            'latestSaleDate': latest_sale_date
        }

        print(f"  ✓ Got profile data: {email}, {cell_phone}, {sales_stats.get('countAllTime', 0)} total sales")
        print(f"  Social media - FB: {bool(corrected_socials['facebook'])}, IG: {bool(corrected_socials['instagram'])}, LI: {bool(corrected_socials['linkedin'])}")

        return result

    except Exception as e:
        print(f"  Error scraping profile: {e}")
        return None

def enrich_realtor_basic(lead):
    """Basic enrichment without Zillow (no proxy usage)"""
    stats = lead.get('stats', {})
    combined = stats.get('combined_annual', {})
    avg_value = 'N/A'
    if combined.get('min') and combined.get('max'):
        avg = (combined['min'] + combined['max']) / 2
        avg_value = f"${int(avg):,}"

    def clean_str(s):
        if not s:
            return ''
        return str(s).replace('"', "'").replace('\n', ' ').replace('\r', ' ').strip()

    return {
        'firstName': clean_str(lead.get('firstName', '')),
        'lastName': clean_str(lead.get('lastName', '')),
        'email': '',
        'phone': '',
        'brokeragePhone': '',
        'yearsExperience': 'Unknown',
        'totalSales': safe_int(lead.get('totalSales', 0)),
        'sales12Months': safe_int(lead.get('sales12Months', 0)),
        'reviewCount': 0,
        'title': '',
        'description': '',
        'specialties': [],
        'serviceAreas': [],
        'businessName': '',
        'businessAddress': '',
        'pronouns': '',
        'websiteUrl': '',
        'latestSaleAddress': '',
        'latestSaleDate': '',
        'recentSales': [],
        'areasWorked': [],
        'avgHomeValue': avg_value,
        'awards': [],
        'profileUrl': clean_str(lead.get('profileUrl', '')),
        'zillowUrl': '',
        'socialMedia': {}
    }

def enrich_csv_lead_with_zillow(lead):
    """For CSV upload: Add Zillow data to existing realtor.com data"""

    first_name = lead.get('firstName', '')
    last_name = lead.get('lastName', '')

    # Get city/state - CSV should have this or we default to Oklahoma City
    city_state = lead.get('cityState', 'oklahoma-city-ok')

    # Get realtor.com 12mo sales from CSV for verification
    realtor_12mo = safe_int(lead.get('sales12Months', 0))

    print(f"Enriching: {first_name} {last_name}")

    # Do Zillow enrichment with Human Journey
    zillow_data = enrich_with_zillow(first_name, last_name, city_state, realtor_12mo)

    if zillow_data:
        # Merge CSV data with Zillow data - zillowUrl MUST come from Zillow, not CSV
        result = {
            **lead,  # Keep all original CSV data
            'email': zillow_data.get('email', lead.get('email', '')),
            'phone': zillow_data.get('phone', lead.get('phone', '')),
            'brokeragePhone': zillow_data.get('brokeragePhone', ''),
            'yearsExperience': f"{zillow_data.get('yearsExperience', 0)} years" if zillow_data.get('yearsExperience') else lead.get('yearsExperience', 'Unknown'),
            'totalSales': zillow_data.get('totalSalesAllTime', lead.get('totalSales', 0)),
            'sales12Months': zillow_data.get('sales12Months', lead.get('sales12Months', 0)),
            'reviewCount': zillow_data.get('reviewCount', 0),
            'title': zillow_data.get('title', ''),
            'description': zillow_data.get('description', ''),
            'specialties': zillow_data.get('specialties', []),
            'serviceAreas': zillow_data.get('serviceAreas', []),
            'businessName': zillow_data.get('businessName', ''),
            'businessAddress': zillow_data.get('businessAddress', ''),
            'pronouns': zillow_data.get('pronouns', ''),
            'websiteUrl': zillow_data.get('websiteUrl', ''),
            'latestSaleAddress': zillow_data.get('latestSaleAddress', ''),
            'latestSaleDate': zillow_data.get('latestSaleDate', ''),
            'avgHomeValue': lead.get('avgHomeValue', 'N/A'),  # Preserve from CSV
            'profileUrl': lead.get('profileUrl', ''),  # Preserve from CSV
            'socialMedia': {
                'facebook': zillow_data.get('facebookUrl', ''),
                'linkedin': zillow_data.get('linkedInUrl', ''),
                'instagram': zillow_data.get('instagramUrl', ''),
                'twitter': zillow_data.get('twitterUrl', ''),
                'youtube': zillow_data.get('youtubeUrl', ''),
                'tiktok': ''
            }
        }
        # Force override zillowUrl AFTER everything else to ensure it's not empty
        result['zillowUrl'] = zillow_data.get('zillowUrl', '')
        return result
    else:
        # Return original CSV data unchanged
        return lead

def enrich_realtor(lead):
    """Enrich realtor data with Zillow info"""

    # Calculate average home value from realtor.com stats
    stats = lead.get('stats', {})
    combined = stats.get('combined_annual', {})
    avg_value = 'N/A'
    if combined.get('min') and combined.get('max'):
        avg = (combined['min'] + combined['max']) / 2
        avg_value = f"${int(avg):,}"

    # Use SAME ultra clean as profile scraping
    def clean_str(s):
        if not s:
            return ''
        s = str(s)
        # Remove newlines and tabs FIRST
        s = s.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        import re
        s = re.sub(r'<[^>]+>', '', s)
        s = re.sub(r'[^a-zA-Z0-9\s@\.\,\-\/\(\)]', '', s)
        return ' '.join(s.split())

    first_name = clean_str(lead.get('firstName', ''))
    last_name = clean_str(lead.get('lastName', ''))

    # Get city/state from lead (format: "Oklahoma-City_OK")
    city_state_slug = lead.get('cityState', 'oklahoma-city-ok')
    # Convert to Zillow format: "Oklahoma-City_OK" -> "oklahoma-city-ok"
    city_state = city_state_slug.lower().replace('_', '-')

    # Enrich with Zillow data (pass realtor 12mo sales for verification)
    realtor_12mo = safe_int(lead.get('sales12Months', 0))
    zillow_data = enrich_with_zillow(first_name, last_name, city_state, realtor_12mo)

    if zillow_data:
        print(f"✓ Using Zillow data")

        # Clean specialty list
        specialties = zillow_data.get('specialties', [])
        clean_specialties = [clean_str(s) for s in specialties] if isinstance(specialties, list) else []

        service_areas_list = zillow_data.get('serviceAreas', [])
        clean_areas = [clean_str(a) for a in service_areas_list] if isinstance(service_areas_list, list) else []

        result = {
            'firstName': first_name,
            'lastName': last_name,
            'email': clean_str(zillow_data.get('email', '')),
            'phone': clean_str(zillow_data.get('phone', '')),
            'brokeragePhone': clean_str(zillow_data.get('brokeragePhone', '')),
            'yearsExperience': f"{zillow_data.get('yearsExperience', 0)} years" if zillow_data.get('yearsExperience') else 'Unknown',
            'totalSales': int(zillow_data.get('totalSalesAllTime', 0) or 0),
            'sales12Months': int(zillow_data.get('sales12Months', 0) or 0),
            'reviewCount': int(zillow_data.get('reviewCount', 0) or 0),
            'title': clean_str(zillow_data.get('title', '')),
            'description': clean_str(zillow_data.get('description', '')),
            'specialties': clean_specialties,
            'serviceAreas': clean_areas,
            'businessName': clean_str(zillow_data.get('businessName', '')),
            'businessAddress': clean_str(zillow_data.get('businessAddress', '')),
            'pronouns': clean_str(zillow_data.get('pronouns', '')),
            'websiteUrl': zillow_data.get('websiteUrl', ''),  # Don't clean URLs!
            'latestSaleAddress': clean_str(zillow_data.get('latestSaleAddress', '')),
            'latestSaleDate': zillow_data.get('latestSaleDate', ''),  # Don't clean dates!
            'recentSales': [],
            'areasWorked': [],
            'avgHomeValue': avg_value,
            'awards': [],
            'profileUrl': lead.get('profileUrl', ''),  # Don't clean URLs!
            'zillowUrl': zillow_data.get('zillowUrl', ''),  # Don't clean URLs!
            'socialMedia': {
                'facebook': zillow_data.get('facebookUrl', ''),  # Don't clean URLs!
                'linkedin': zillow_data.get('linkedInUrl', ''),  # Don't clean URLs!
                'instagram': zillow_data.get('instagramUrl', ''),  # Don't clean URLs!
                'twitter': zillow_data.get('twitterUrl', ''),  # Don't clean URLs!
                'youtube': zillow_data.get('youtubeUrl', ''),  # Don't clean URLs!
                'tiktok': ''
            }
        }
    else:
        print(f"✗ No Zillow data, using realtor.com stats")
        result = {
            'firstName': first_name,
            'lastName': last_name,
            'email': '',
            'phone': '',
            'brokeragePhone': '',
            'yearsExperience': 'Unknown',
            'totalSales': safe_int(lead.get('totalSales', 0)),
            'sales12Months': safe_int(lead.get('sales12Months', 0)),
            'reviewCount': 0,
            'title': '',
            'description': '',
            'specialties': [],
            'serviceAreas': [],
            'businessName': '',
            'businessAddress': '',
            'pronouns': '',
            'websiteUrl': '',
            'recentSales': [],
            'areasWorked': [],
            'avgHomeValue': avg_value,
            'awards': [],
            'profileUrl': clean_str(lead.get('profileUrl', '')),
            'zillowUrl': '',
            'socialMedia': {}
        }

    return result

@app.route('/api/export', methods=['POST'])
def export_csv():
    data = request.json
    results = data.get('results', [])

    # Create CSV
    output = io.StringIO()
    if results:
        fieldnames = ['firstName', 'lastName', 'email', 'phone', 'brokeragePhone',
                     'yearsExperience', 'totalSales', 'sales12Months', 'reviewCount', 'title', 'description',
                     'specialties', 'serviceAreas', 'businessName', 'businessAddress', 'pronouns', 'websiteUrl',
                     'latestSaleAddress', 'latestSaleDate', 'avgHomeValue', 'profileUrl', 'zillowUrl',
                     'facebook', 'linkedin', 'instagram', 'twitter', 'youtube', 'tiktok']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for result in results:
            row = {
                'firstName': result.get('firstName', ''),
                'lastName': result.get('lastName', ''),
                'email': result.get('email', ''),
                'phone': result.get('phone', ''),
                'brokeragePhone': result.get('brokeragePhone', ''),
                'yearsExperience': result.get('yearsExperience', ''),
                'totalSales': result.get('totalSales', 0),
                'sales12Months': result.get('sales12Months', 0),
                'reviewCount': result.get('reviewCount', 0),
                'title': result.get('title', ''),
                'description': result.get('description', '').replace('\n', ' ').strip(),
                'specialties': '; '.join(result.get('specialties', [])),
                'serviceAreas': '; '.join(result.get('serviceAreas', [])),
                'businessName': result.get('businessName', ''),
                'businessAddress': result.get('businessAddress', ''),
                'pronouns': result.get('pronouns', ''),
                'websiteUrl': result.get('websiteUrl', ''),
                'latestSaleAddress': result.get('latestSaleAddress', ''),
                'latestSaleDate': result.get('latestSaleDate', ''),
                'avgHomeValue': result.get('avgHomeValue', ''),
                'profileUrl': result.get('profileUrl', ''),
                'zillowUrl': result.get('zillowUrl', ''),
                'facebook': result.get('socialMedia', {}).get('facebook', ''),
                'linkedin': result.get('socialMedia', {}).get('linkedin', ''),
                'instagram': result.get('socialMedia', {}).get('instagram', ''),
                'twitter': result.get('socialMedia', {}).get('twitter', ''),
                'youtube': result.get('socialMedia', {}).get('youtube', ''),
                'tiktok': result.get('socialMedia', {}).get('tiktok', '')
            }
            writer.writerow(row)

    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode('utf-8')),
        mimetype='text/csv',
        as_attachment=True,
        download_name=f'realtor-leads-{datetime.now().strftime("%Y%m%d")}.csv'
    )

if __name__ == '__main__':
    app.run(debug=True, port=5000)
