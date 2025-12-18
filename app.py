from flask import Flask, render_template, request, jsonify, send_file
import requests
import json
import csv
import io
import time
import re
import uuid
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests

app = Flask(__name__)

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

# Chrome 120 headers - matches TLS impersonation
CHROME_120_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'sec-ch-ua': '"Not_A Brand";v="99", "Chromium";v="120"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1'
}

print(f"Using ProxyJet rotating proxies with sticky sessions")

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
        """Stream results as they're fetched - in batches"""
        if mode == 'area':
            areas = data.get('areas', [])
            for area in areas:
                batch = []
                enriched_count = 0
                max_enrich = 10  # TEST: Increased to 10 to see pattern
                total_agents = 0

                # Stream agents in batches of 10 (smaller batches = more reliable)
                for agent in stream_agents_from_area(area):
                    if agent is None:
                        # Keepalive
                        yield ": keepalive\n\n"
                        continue

                    total_agents += 1

                    try:
                        # Only enrich first 5 with Zillow (TEST MODE)
                        if enriched_count < max_enrich:
                            print(f"")
                            print(f"========================================")
                            print(f"ZILLOW ENRICHMENT #{enriched_count + 1} OF {max_enrich}")
                            print(f"========================================")
                            enriched = enrich_realtor(agent)
                            enriched_count += 1
                        else:
                            # Skip Zillow enrichment for rest
                            if total_agents == max_enrich + 1:
                                print(f"")
                                print(f"*** STOPPING ZILLOW ENRICHMENT - REACHED {max_enrich} LIMIT ***")
                                print(f"*** Remaining {5291 - max_enrich} agents will get basic data only ***")
                                print(f"")
                            enriched = enrich_realtor_basic(agent)

                        batch.append(enriched)

                        # Send batch when we have 10
                        if len(batch) >= 10:
                            try:
                                json_str = json.dumps({'batch': batch}, ensure_ascii=True, separators=(',', ':'))
                                yield f"data: {json_str}\n\n"
                                batch = []
                            except Exception as je:
                                print(f"JSON encoding error, sending one by one: {je}")
                                # Fallback: send individually
                                for item in batch:
                                    try:
                                        single_json = json.dumps(item, ensure_ascii=True, separators=(',', ':'))
                                        yield f"data: {single_json}\n\n"
                                    except:
                                        pass
                                batch = []

                    except Exception as e:
                        print(f"Error encoding agent: {e}")
                        continue

                # Send remaining batch
                if batch:
                    try:
                        json_str = json.dumps({'batch': batch}, ensure_ascii=True, separators=(',', ':'))
                        yield f"data: {json_str}\n\n"
                    except:
                        for item in batch:
                            try:
                                single_json = json.dumps(item, ensure_ascii=True, separators=(',', ':'))
                                yield f"data: {single_json}\n\n"
                            except:
                                pass

        elif mode == 'csv':
            leads = data.get('leads', [])
            for lead in leads:
                if lead.get('firstName') and lead.get('lastName'):
                    try:
                        enriched = enrich_realtor(lead)
                        json_str = json.dumps(enriched, ensure_ascii=True, separators=(',', ':'))
                        yield f"data: {json_str}\n\n"
                        time.sleep(0.5)
                    except Exception as e:
                        print(f"Error encoding lead: {e}")
                        continue

        yield 'data: {"done":true}\n\n'

    return app.response_class(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no'
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
    try:
        # Generate unique session ID for this agent (sticky IP)
        run_session_id = str(uuid.uuid4())[:8]
        sticky_proxy_url = get_sticky_proxy_url(run_session_id)
        proxies = {'http': sticky_proxy_url, 'https': sticky_proxy_url}

        print(f"Starting Human Journey for {first_name} {last_name} (session: {run_session_id})")

        # Create ONE session for entire journey
        session = curl_requests.Session()

        # STEP 1: Visit homepage (lander) to get cookies
        print(f"  Step 1: Landing on homepage...")
        home_response = session.get(
            'https://www.zillow.com/',
            headers=CHROME_120_HEADERS.copy(),
            impersonate="chrome120",
            proxies=proxies,
            timeout=20
        )

        if home_response.status_code != 200:
            print(f"  Homepage failed: {home_response.status_code}, aborting")
            session.close()
            return None

        print(f"  ✓ Homepage cookies collected")
        time.sleep(random.uniform(3.0, 5.0))  # Look like browsing

        # STEP 2: Search for agent
        full_name_realtor = f"{first_name} {last_name}".strip()
        from urllib.parse import quote_plus
        name_query = quote_plus(full_name_realtor)
        search_url = f"https://www.zillow.com/professionals/real-estate-agent-reviews/{city_state.lower().replace(' ', '-')}/?name={name_query}"

        print(f"  Step 2: Searching for {full_name_realtor}...")

        search_headers = CHROME_120_HEADERS.copy()
        search_headers['Referer'] = 'https://www.zillow.com/'

        search_response = session.get(
            search_url,
            headers=search_headers,
            impersonate="chrome120",
            proxies=proxies,
            timeout=20
        )

        if search_response.status_code != 200:
            print(f"  Search failed: {search_response.status_code}, aborting")
            session.close()
            return None

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

        # Find matching agent with sales verification
        realtor_clean = clean_for_match(full_name_realtor)
        name_matches = []

        # Collect all name matches
        for card in results:
            if card.get('__typename') == 'AgentDirectoryFinderProfileResultsCard':
                card_name = card.get('cardTitle', '')
                zillow_clean = clean_for_match(card_name)

                # Bidirectional match: does realtor contain zillow OR zillow contain realtor
                if realtor_clean in zillow_clean or zillow_clean in realtor_clean:
                    # Extract Zillow 12mo sales
                    profile_data = card.get('profileData', [])
                    zillow_12mo = 0
                    for item in profile_data:
                        if 'sales last 12 months' in item.get('label', '').lower():
                            zillow_12mo = int(item.get('data') or 0)
                            break

                    name_matches.append({
                        'card': card,
                        'name': card_name,
                        'zillow_12mo': zillow_12mo
                    })

        if not name_matches:
            print(f"No Zillow name match for '{full_name_realtor}'")
            return None

        # Try to find match within 10 sales
        best_match = None
        for match in name_matches:
            sales_diff = abs(match['zillow_12mo'] - realtor_12mo_sales)
            print(f"  '{match['name']}': Zillow 12mo={match['zillow_12mo']}, Realtor 12mo={realtor_12mo_sales}, diff={sales_diff}")

            if sales_diff <= 10:
                best_match = match['card']
                print(f"  ✓ VERIFIED MATCH (sales within 10)")
                break

        # If no match within 10, use first result
        if not best_match:
            best_match = name_matches[0]['card']
            print(f"  Using first match (no sales match within 10)")

        # Extract profile data from matched card
        profile_data = best_match.get('profileData', [])
        zillow_url = best_match.get('cardActionLink', '')

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
            return {
                'zillowUrl': zillow_url,
                'totalSalesInCity': total_sales_in_city,
                'sales12Months': sales_last_12mo,
                **profile_data  # Merge profile data
            }

        return {
            'zillowUrl': zillow_url,
            'totalSalesInCity': total_sales_in_city,
            'sales12Months': sales_last_12mo
        }
    except Exception as e:
        print(f"Error in Human Journey for {first_name} {last_name}: {e}")
        import traceback
        traceback.print_exc()
        return None

def scrape_zillow_profile_journey(session, profile_url, search_url, proxies):
    """STEP 3: Visit profile page with same session"""
    try:
        print(f"  Visiting profile: {profile_url}")

        # Use SAME session with cookies from homepage + search
        profile_headers = CHROME_120_HEADERS.copy()
        profile_headers['Referer'] = search_url  # Came from search results!

        response = session.get(
            profile_url,
            headers=profile_headers,
            impersonate="chrome120",
            proxies=proxies,
            timeout=20
        )

        if response.status_code != 200:
            print(f"  Profile returned status {response.status_code}")
            return None

        print(f"  ✓ Profile success! Status 200")

        soup = BeautifulSoup(response.content, 'html.parser')

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
            # Remove HTML
            import re
            s = re.sub(r'<[^>]+>', '', str(s))
            # Keep ONLY: letters, numbers, spaces, @ . , -
            s = re.sub(r'[^a-zA-Z0-9\s@\.\,\-]', '', s)
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
            latest_sale_date = clean_str(latest.get('sold_date', ''))

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
            'websiteUrl': clean_str(get_to_know.get('websiteUrl', '')),
            'facebookUrl': clean_str(get_to_know.get('facebookUrl', '')),
            'linkedInUrl': clean_str(get_to_know.get('linkedInUrl', '')),
            'latestSaleAddress': latest_sale_address,
            'latestSaleDate': latest_sale_date
        }

        print(f"  ✓ Got profile data: {email}, {cell_phone}, {sales_stats.get('countAllTime', 0)} total sales")

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
        'totalSales': int(lead.get('totalSales', 0)),
        'sales12Months': int(lead.get('sales12Months', 0)),
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

def enrich_realtor(lead):
    """Enrich realtor data with Zillow info"""

    # Calculate average home value from realtor.com stats
    stats = lead.get('stats', {})
    combined = stats.get('combined_annual', {})
    avg_value = 'N/A'
    if combined.get('min') and combined.get('max'):
        avg = (combined['min'] + combined['max']) / 2
        avg_value = f"${int(avg):,}"

    # Clean strings
    def clean_str(s):
        if not s:
            return ''
        return str(s).replace('"', "'").replace('\n', ' ').replace('\r', ' ').strip()

    first_name = clean_str(lead.get('firstName', ''))
    last_name = clean_str(lead.get('lastName', ''))

    # Get city/state from lead (format: "Oklahoma-City_OK")
    city_state_slug = lead.get('cityState', 'oklahoma-city-ok')
    # Convert to Zillow format: "Oklahoma-City_OK" -> "oklahoma-city-ok"
    city_state = city_state_slug.lower().replace('_', '-')

    # Enrich with Zillow data (pass realtor 12mo sales for verification)
    realtor_12mo = int(lead.get('sales12Months', 0))
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
            'websiteUrl': clean_str(zillow_data.get('websiteUrl', '')),
            'latestSaleAddress': clean_str(zillow_data.get('latestSaleAddress', '')),
            'latestSaleDate': clean_str(zillow_data.get('latestSaleDate', '')),
            'recentSales': [],
            'areasWorked': [],
            'avgHomeValue': avg_value,
            'awards': [],
            'profileUrl': clean_str(lead.get('profileUrl', '')),
            'zillowUrl': clean_str(zillow_data.get('zillowUrl', '')),
            'socialMedia': {
                'facebook': clean_str(zillow_data.get('facebookUrl', '')),
                'linkedin': clean_str(zillow_data.get('linkedInUrl', '')),
                'instagram': '',
                'twitter': '',
                'youtube': '',
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
            'totalSales': int(lead.get('totalSales', 0)),
            'sales12Months': int(lead.get('sales12Months', 0)),
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
