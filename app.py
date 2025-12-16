from flask import Flask, render_template, request, jsonify, send_file
import requests
import json
import csv
import io
import time
from datetime import datetime

app = Flask(__name__)

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
        """Stream results as they're fetched"""
        if mode == 'area':
            areas = data.get('areas', [])
            for area in areas:
                # Stream agents as they're found
                for agent in stream_agents_from_area(area):
                    try:
                        enriched = enrich_realtor(agent)
                        # Ensure proper JSON encoding - compact, no newlines
                        json_str = json.dumps(enriched, ensure_ascii=True, separators=(',', ':'))
                        yield f"data: {json_str}\n\n"
                    except Exception as e:
                        print(f"Error encoding agent: {e}")
                        continue

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
                # Parse agent data
                full_name = agent.get('fullname', '')
                name_parts = full_name.split()

                first_name = name_parts[0] if name_parts else ''
                last_name = ' '.join(name_parts[1:]) if len(name_parts) > 1 else ''

                # Build profile URL from fulfillment_id
                fulfillment_id = agent.get('fulfillment_id', '')
                profile_url = f"https://www.realtor.com/realestateagents/{fulfillment_id}" if fulfillment_id else ''

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
                    'fulfillment_id': fulfillment_id,
                    'sales12Months': sales_12mo,
                    'totalSales': for_sale_count,
                    'recentSales': recent_sales_data[:5],
                    'stats': stats
                }

            # Move to next page
            offset += limit

            # Stop if we've fetched all available agents
            if offset >= total_rows:
                print(f"Fetched all {total_rows} agents, stopping")
                break

            # No sleep - stream as fast as possible
            # time.sleep(0.5)

        print(f"Streaming complete")

    except Exception as e:
        print(f"Error searching agents: {e}")
        import traceback
        traceback.print_exc()

def enrich_realtor(lead):
    """Enrich realtor data"""

    # Format recent sales from API data
    recent_sales = []
    raw_sales = lead.get('recentSales', [])
    for sale in raw_sales[:5]:
        recent_sales.append({
            'address': f"{sale.get('city', '')}, {sale.get('state_code', '')}",
            'date': 'Recently sold',
            'price': f"{sale.get('beds', 'N/A')} bed, {sale.get('baths', 'N/A')} bath"
        })

    # Calculate average home value from stats
    stats = lead.get('stats', {})
    combined = stats.get('combined_annual', {})
    avg_value = 'N/A'
    if combined.get('min') and combined.get('max'):
        avg = (combined['min'] + combined['max']) / 2
        avg_value = f"${int(avg):,}"

    result = {
        'firstName': lead.get('firstName', ''),
        'lastName': lead.get('lastName', ''),
        'email': lead.get('email', ''),
        'phone': lead.get('phone', ''),
        'yearsExperience': 'Unknown',
        'totalSales': lead.get('totalSales', 0),
        'sales12Months': lead.get('sales12Months', 0),
        'recentSales': recent_sales,
        'areasWorked': [],
        'avgHomeValue': avg_value,
        'specializations': [],
        'awards': [],
        'profileUrl': lead.get('profileUrl', ''),
        'socialMedia': {
            'facebook': '',
            'linkedin': '',
            'instagram': '',
            'twitter': '',
            'youtube': '',
            'tiktok': ''
        }
    }

    return result

@app.route('/api/export', methods=['POST'])
def export_csv():
    data = request.json
    results = data.get('results', [])

    # Create CSV
    output = io.StringIO()
    if results:
        fieldnames = ['firstName', 'lastName', 'email', 'phone', 'yearsExperience',
                     'totalSales', 'sales12Months', 'areasWorked', 'avgHomeValue',
                     'specializations', 'awards', 'profileUrl', 'facebook',
                     'linkedin', 'instagram', 'twitter', 'youtube', 'tiktok']
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for result in results:
            row = {
                'firstName': result.get('firstName', ''),
                'lastName': result.get('lastName', ''),
                'email': result.get('email', ''),
                'phone': result.get('phone', ''),
                'yearsExperience': result.get('yearsExperience', ''),
                'totalSales': result.get('totalSales', 0),
                'sales12Months': result.get('sales12Months', 0),
                'areasWorked': '; '.join(result.get('areasWorked', [])),
                'avgHomeValue': result.get('avgHomeValue', ''),
                'specializations': '; '.join(result.get('specializations', [])),
                'awards': '; '.join(result.get('awards', [])),
                'profileUrl': result.get('profileUrl', ''),
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
