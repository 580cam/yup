from flask import Flask, render_template, request, jsonify, send_file
import requests
import json
import csv
import io
import time
import re
from datetime import datetime
from bs4 import BeautifulSoup
from curl_cffi import requests as curl_requests

app = Flask(__name__)

# Use curl-cffi for Zillow (impersonates real browser TLS)
zillow_session = curl_requests.Session()

# ProxyJet credentials
PROXY_USERNAME = "251216vin3B-resi-US"  # rotating residential US
PROXY_PASSWORD = "Ib4MCO7Q8YIsylv"
PROXY_SERVER = "proxy-jet.io:1010"
PROXY_URL = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{PROXY_SERVER}"

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
                max_enrich = 5  # TEST: Only enrich first 5 agents
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
        # Initialize Zillow session to get cookies
        init_zillow_session()

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

        response = requests.post(GRAPHQL_URL, json=location_query, headers=HEADERS, timeout=15)

        print(f"Location search status: {response.status_code}")

        location_data = response.json()

        if 'data' in location_data and 'agents_location_search' in location_data['data']:
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

        zillow_session.get('https://www.zillow.com/', impersonate="chrome120", proxies=proxies, timeout=20)
        print(f"Zillow session initialized (ProxyJet residential proxy)")
    except Exception as e:
        print(f"Failed to init Zillow session: {e}")
        import traceback
        traceback.print_exc()

def enrich_with_zillow(first_name, last_name, city_state):
    """Search Zillow for agent and extract email, phone, total sales"""
    try:
        # Combine full name from realtor.com
        full_name_realtor = f"{first_name} {last_name}".strip()

        # URL encode the names
        from urllib.parse import quote_plus
        name_query = quote_plus(full_name_realtor)

        search_url = f"https://www.zillow.com/professionals/real-estate-agent-reviews/{city_state.lower().replace(' ', '-')}/?name={name_query}"

        print(f"Searching Zillow for: {full_name_realtor}")

        # Zillow-specific headers to avoid 403
        zillow_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': f'https://www.zillow.com/professionals/real-estate-agent-reviews/{city_state.lower().replace(" ", "-")}/',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="143"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1'
        }

        # Use ProxyJet rotating proxy - auto-rotates to new residential IP
        proxies = {'http': PROXY_URL, 'https': PROXY_URL}

        # Use curl-cffi to impersonate real Chrome browser (TLS fingerprint)
        # Only fetch HTML, don't load images/CSS/JS
        response = zillow_session.get(
            search_url,
            headers=zillow_headers,
            impersonate="chrome120",
            proxies=proxies,
            timeout=20,
            allow_redirects=True,
            max_redirects=3
        )

        if response.status_code != 200:
            print(f"Zillow returned status {response.status_code}")
            return None

        print(f"Zillow success! Status 200")

        soup = BeautifulSoup(response.content, 'html.parser')

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

        # Find matching agent - bidirectional phrase match
        best_match = None
        realtor_clean = clean_for_match(full_name_realtor)

        for card in results:
            if card.get('__typename') == 'AgentDirectoryFinderProfileResultsCard':
                card_name = card.get('cardTitle', '')
                zillow_clean = clean_for_match(card_name)

                # Bidirectional match: does realtor contain zillow OR zillow contain realtor
                if realtor_clean in zillow_clean or zillow_clean in realtor_clean:
                    best_match = card
                    print(f"MATCH FOUND: '{card_name}' ≈ '{full_name_realtor}' (cleaned: '{zillow_clean}' ≈ '{realtor_clean}')")
                    break  # Take first match

        if not best_match:
            print(f"No Zillow match for '{full_name_realtor}' (cleaned: '{realtor_clean}')")
            return None

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

        # Now scrape the actual profile page for full details
        profile_data = scrape_zillow_profile(zillow_url)

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
        print(f"Error enriching with Zillow for {first_name} {last_name}: {e}")
        import traceback
        traceback.print_exc()
        return None

def scrape_zillow_profile(profile_url):
    """Scrape detailed info from Zillow profile page"""
    try:
        print(f"  Scraping profile: {profile_url}")

        # Use ProxyJet proxy with new session to avoid 403
        proxies = {'http': PROXY_URL, 'https': PROXY_URL}

        # Set referer to look like we came from search page
        profile_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.zillow.com/professionals/real-estate-agent-reviews/',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        }

        response = zillow_session.get(profile_url, headers=profile_headers, impersonate="chrome120", proxies=proxies, timeout=20)

        if response.status_code != 200:
            print(f"  Profile returned status {response.status_code}")
            return None

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

        email = display_user.get('email', '')
        phone_numbers = display_user.get('phoneNumbers', {})
        cell_phone = phone_numbers.get('cell', '')
        brokerage_phone = phone_numbers.get('brokerage', '')
        business_address = display_user.get('businessAddress', {})
        full_address = f"{business_address.get('address1', '')}, {business_address.get('city', '')}, {business_address.get('state', '')} {business_address.get('postalCode', '')}"

        result = {
            'email': email,
            'phone': cell_phone,
            'brokeragePhone': brokerage_phone,
            'totalSalesAllTime': sales_stats.get('countAllTime', 0),
            'yearsExperience': get_to_know.get('yearsInIndustry', 0),
            'title': get_to_know.get('title', ''),
            'description': get_to_know.get('description', ''),
            'specialties': get_to_know.get('specialties', []),
            'businessName': display_user.get('businessName', ''),
            'businessAddress': full_address.strip(', '),
            'pronouns': display_user.get('cpdUserPronouns', ''),
            'websiteUrl': get_to_know.get('websiteUrl', ''),
            'facebookUrl': get_to_know.get('facebookUrl', ''),
            'linkedInUrl': get_to_know.get('linkedInUrl', '')
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
        'yearsExperience': 'Unknown',
        'totalSales': int(lead.get('totalSales', 0)),
        'sales12Months': int(lead.get('sales12Months', 0)),
        'recentSales': [],
        'areasWorked': [],
        'avgHomeValue': avg_value,
        'specializations': [],
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

    # Enrich with Zillow data
    zillow_data = enrich_with_zillow(first_name, last_name, city_state)

    if zillow_data:
        print(f"✓ Using Zillow data")
        result = {
            'firstName': first_name,
            'lastName': last_name,
            'email': zillow_data.get('email', ''),
            'phone': zillow_data.get('phone', ''),
            'brokeragePhone': zillow_data.get('brokeragePhone', ''),
            'yearsExperience': f"{zillow_data.get('yearsExperience', 0)} years" if zillow_data.get('yearsExperience') else 'Unknown',
            'totalSales': zillow_data.get('totalSalesAllTime', 0),
            'sales12Months': zillow_data.get('sales12Months', 0),
            'title': zillow_data.get('title', ''),
            'description': zillow_data.get('description', ''),
            'specialties': zillow_data.get('specialties', []),
            'businessName': zillow_data.get('businessName', ''),
            'businessAddress': zillow_data.get('businessAddress', ''),
            'pronouns': zillow_data.get('pronouns', ''),
            'websiteUrl': zillow_data.get('websiteUrl', ''),
            'recentSales': [],
            'areasWorked': [],
            'avgHomeValue': avg_value,
            'awards': [],
            'profileUrl': clean_str(lead.get('profileUrl', '')),
            'zillowUrl': zillow_data.get('zillowUrl', ''),
            'socialMedia': {
                'facebook': zillow_data.get('facebookUrl', ''),
                'linkedin': zillow_data.get('linkedInUrl', ''),
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
            'title': '',
            'description': '',
            'specialties': [],
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
                     'yearsExperience', 'totalSales', 'sales12Months', 'title', 'description',
                     'specialties', 'businessName', 'businessAddress', 'pronouns', 'websiteUrl',
                     'avgHomeValue', 'profileUrl', 'zillowUrl', 'facebook', 'linkedin',
                     'instagram', 'twitter', 'youtube', 'tiktok']
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
                'title': result.get('title', ''),
                'description': result.get('description', '').replace('\n', ' ').strip(),
                'specialties': '; '.join(result.get('specialties', [])),
                'businessName': result.get('businessName', ''),
                'businessAddress': result.get('businessAddress', ''),
                'pronouns': result.get('pronouns', ''),
                'websiteUrl': result.get('websiteUrl', ''),
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
