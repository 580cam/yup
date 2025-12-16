from flask import Flask, render_template, request, jsonify, send_file
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import csv
import io
import time
from datetime import datetime

app = Flask(__name__)

def get_driver():
    """Create undetected Chrome driver"""
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-gpu')

    try:
        # Try system chromium first (for Railway/Docker)
        driver = uc.Chrome(options=options, driver_executable_path='/usr/bin/chromedriver')
    except:
        # Fallback to auto-download
        driver = uc.Chrome(options=options, version_main=131)

    return driver

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/scrape', methods=['POST'])
def scrape():
    data = request.json
    mode = data.get('mode')

    results = []

    if mode == 'area':
        areas = data.get('areas', [])
        for area in areas:
            # Scrape realtors from area
            realtors = find_realtors_in_area(area)
            for realtor in realtors:
                enriched = enrich_realtor(realtor)
                results.append(enriched)
                time.sleep(1)  # Rate limiting

    elif mode == 'csv':
        leads = data.get('leads', [])
        for lead in leads:
            if lead.get('firstName') and lead.get('lastName'):
                enriched = enrich_realtor(lead)
                results.append(enriched)
                time.sleep(1)  # Rate limiting

    return jsonify({'results': results})

def find_realtors_in_area(area):
    """Find realtors in target area using undetected Chrome"""
    realtors = []
    driver = None

    try:
        driver = get_driver()
        url = f"https://www.realtor.com/realestateagents/{area.replace(' ', '-').replace(',', '')}"
        print(f"Fetching: {url}")

        driver.get(url)
        time.sleep(3)  # Wait for page load

        # Get page source after JS renders
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Look for agent profile links
        all_links = soup.find_all('a', href=True)

        seen_names = set()
        for link in all_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)

            # Look for profile links
            if '/realestateagents/' in href and '/profile/' in href and text and len(text.split()) >= 2:
                full_name = text.strip()

                # Skip if not a name
                if len(full_name) > 50 or full_name in seen_names or not full_name[0].isupper():
                    continue

                name_parts = full_name.split()
                if len(name_parts) < 2:
                    continue

                firstName = name_parts[0]
                lastName = ' '.join(name_parts[1:])

                # Build full URL
                profile_url = href if href.startswith('http') else f"https://www.realtor.com{href}"

                seen_names.add(full_name)

                realtors.append({
                    'firstName': firstName,
                    'lastName': lastName,
                    'profileUrl': profile_url,
                    'phone': '',
                    'source': 'realtor.com'
                })

                if len(realtors) >= 20:
                    break

        print(f"Found {len(realtors)} realtors")

    except Exception as e:
        print(f"Error scraping area {area}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()

    return realtors

def enrich_realtor(lead):
    """Enrich realtor data from their profile page"""
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

    profile_url = lead.get('profileUrl')
    if not profile_url:
        return result

    driver = None
    try:
        driver = get_driver()
        driver.get(profile_url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # Extract experience
        exp_text = soup.get_text()
        if 'year' in exp_text.lower():
            import re
            match = re.search(r'(\d+)\s*(?:years?|yrs?)', exp_text, re.IGNORECASE)
            if match:
                result['yearsExperience'] = f"{match.group(1)} years"

        # Extract sales numbers
        numbers = soup.find_all(string=lambda x: x and any(char.isdigit() for char in str(x)))
        for text in numbers:
            if 'transaction' in str(text).lower() or 'sale' in str(text).lower():
                import re
                match = re.search(r'(\d+)', str(text))
                if match and result['totalSales'] == 0:
                    result['totalSales'] = int(match.group(1))

            if '12' in str(text) and 'month' in str(text).lower():
                import re
                match = re.search(r'(\d+)', str(text))
                if match:
                    result['sales12Months'] = int(match.group(1))

        # Extract email
        email_elem = soup.find('a', href=lambda x: x and x.startswith('mailto:'))
        if email_elem:
            result['email'] = email_elem['href'].replace('mailto:', '')

        # Extract phone
        if not result['phone']:
            phone_elem = soup.find('a', href=lambda x: x and x.startswith('tel:'))
            if phone_elem:
                result['phone'] = phone_elem.get_text(strip=True)

        # Extract social media
        social_links = soup.find_all('a', href=True)
        for link in social_links:
            href = link['href']
            if 'facebook.com' in href:
                result['socialMedia']['facebook'] = href
            elif 'linkedin.com' in href:
                result['socialMedia']['linkedin'] = href
            elif 'instagram.com' in href:
                result['socialMedia']['instagram'] = href
            elif 'twitter.com' in href or 'x.com' in href:
                result['socialMedia']['twitter'] = href
            elif 'youtube.com' in href:
                result['socialMedia']['youtube'] = href
            elif 'tiktok.com' in href:
                result['socialMedia']['tiktok'] = href

    except Exception as e:
        print(f"Error enriching {lead.get('firstName')} {lead.get('lastName')}: {e}")
    finally:
        if driver:
            driver.quit()

    return result

def search_for_profile(search_name):
    """Search for realtor profile"""
    try:
        url = f"https://www.realtor.com/realestateagents/{search_name.replace(' ', '-')}"
        response = requests.get(url, headers=HEADERS, timeout=10)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find first profile link
        profile_link = soup.find('a', href=lambda x: x and '/realestateagents/' in x and '/profile/' in x)
        if profile_link:
            url = profile_link['href']
            if not url.startswith('http'):
                url = 'https://www.realtor.com' + url
            return url
    except:
        pass

    return None

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
