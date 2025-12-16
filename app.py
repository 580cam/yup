from flask import Flask, render_template, request, jsonify, send_file
import requests
import csv
import io
import time
from datetime import datetime

app = Flask(__name__)

GRAPHQL_URL = "https://www.realtor.com/frontdoor/graphql"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json'
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

    results = []

    if mode == 'area':
        areas = data.get('areas', [])
        for area in areas:
            # Get agents from GraphQL API
            agents = find_agents_graphql(area)
            print(f"Found {len(agents)} agents in {area}")

            for agent in agents:
                enriched = enrich_realtor(agent)
                results.append(enriched)
                time.sleep(0.5)  # Light rate limiting

    elif mode == 'csv':
        leads = data.get('leads', [])
        for lead in leads:
            if lead.get('firstName') and lead.get('lastName'):
                enriched = enrich_realtor(lead)
                results.append(enriched)
                time.sleep(0.5)

    return jsonify({'results': results})

def find_agents_graphql(area):
    """Find agents using realtor.com GraphQL API"""
    agents = []

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
        location_data = response.json()

        if 'data' in location_data and 'agents_location_search' in location_data['data']:
            locations = location_data['data']['agents_location_search']['auto_complete']

            if locations:
                # Use first location match
                location = locations[0]
                slug_id = location.get('slug_id')

                print(f"Found location: {slug_id}")

                # Now search for agents in that location
                agents = search_agents_in_location(slug_id)

    except Exception as e:
        print(f"Error finding agents via GraphQL: {e}")
        import traceback
        traceback.print_exc()

    return agents

def search_agents_in_location(slug_id):
    """Search for agents in a specific location"""
    agents = []

    try:
        # Query for agents - this might need adjustment based on their actual schema
        agents_query = {
            "operationName": "AgentSearch",
            "variables": {
                "slug_id": slug_id,
                "limit": 50,
                "offset": 0
            },
            "query": """query AgentSearch($slug_id: String!, $limit: Int, $offset: Int) {
                agent_search(query: {location: {slug_id: $slug_id}, limit: $limit, offset: $offset}) {
                    results {
                        advertiser_id
                        person {
                            name
                            first_name
                            last_name
                            email
                            phone
                        }
                        broker {
                            name
                        }
                        web_url
                        __typename
                    }
                    __typename
                }
            }"""
        }

        response = requests.post(GRAPHQL_URL, json=agents_query, headers=HEADERS, timeout=15)
        data = response.json()

        print(f"Agent search response: {data}")

        # Parse results - exact structure TBD
        # For now, return empty and we'll adjust based on actual response

    except Exception as e:
        print(f"Error searching agents: {e}")
        import traceback
        traceback.print_exc()

    return agents

def enrich_realtor(lead):
    """Enrich realtor data"""
    result = {
        'firstName': lead.get('firstName', ''),
        'lastName': lead.get('lastName', ''),
        'email': lead.get('email', ''),
        'phone': lead.get('phone', ''),
        'yearsExperience': 'Unknown',
        'totalSales': 0,
        'sales12Months': 0,
        'recentSales': [],
        'areasWorked': [],
        'avgHomeValue': 'N/A',
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

    # If we have a profile URL, fetch more details
    # For now, return basic info

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
