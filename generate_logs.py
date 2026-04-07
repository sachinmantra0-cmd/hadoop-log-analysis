#!/usr/bin/env python3
"""
Generate Extended Web Server Logs Dataset
Generates realistic Apache/NASA-style web server logs with legitimate users, bot traffic, and attack attempts
"""

import csv
from datetime import datetime, timedelta
import random

# Configuration
OUTPUT_FILE = 'extended_server_logs.csv'
NUM_RECORDS = 95

# IP addresses
LEGITIMATE_IPS = [f'192.168.1.{i}' for i in range(1, 11)]  # 10 legitimate users
BOT_IPS = ['198.51.100.99']  # 1 bot
ATTACKER_IPS = ['203.0.113.45']  # 1 attacker

# Resources/Pages
RESOURCES = [
    '/index.html',
    '/about.html',
    '/products.html',
    '/services.html',
    '/blog.html',
    '/contact.html',
    '/admin.php',
    '/config.php',
    '/database.sql',
    '/shell.php',
    '/upload.php',
    '/wp-admin.php',
    '/login.php',
    '/phpmyadmin.php',
    '/cpanel.php',
    '/passwd',
    '/shadow',
    '/etc.passwd',
    '/../../admin',
    '/sql.php',
    '/inject.php',
    '/xmlrpc.php',
    '/backup.php'
]

# HTTP Methods
METHODS = ['GET', 'POST', 'PUT', 'DELETE']

# Size ranges (bytes)
PAGE_SIZES = {
    '/index.html': 1245,
    '/about.html': 2341,
    '/products.html': 5678,
    '/services.html': 4123,
    '/blog.html': 3421,
    '/contact.html': 512,
}

# Status codes and their patterns
STATUS_CODES = {
    'success': 200,
    'created': 201,
    'forbidden': 403,
}


def generate_timestamp(base_date, hour_offset):
    """Generate timestamp within specific hour"""
    base = datetime.strptime('2026-03-22 08:00:00', '%Y-%m-%d %H:%M:%S')
    dt = base + timedelta(hours=hour_offset) + timedelta(minutes=random.randint(0, 59))
    dt = dt + timedelta(seconds=random.randint(0, 59))
    return dt.strftime('%Y-%m-%d %H:%M:%S')


def get_page_size(resource):
    """Get size for a given resource"""
    if resource in PAGE_SIZES:
        return PAGE_SIZES[resource]
    elif 'admin' in resource.lower() or '403' in resource:
        return 0  # Forbidden pages have no content
    else:
        return random.randint(1000, 10000)


def generate_logs():
    """Generate web server logs"""
    records = []
    
    # Track distribution
    legitimate_count = 0
    bot_count = 0
    attacker_count = 0
    
    # Generate legitimate user traffic (60 records)
    print("Generating legitimate user traffic...")
    for i in range(60):
        ip = random.choice(LEGITIMATE_IPS)
        user = f'user{LEGITIMATE_IPS.index(ip) + 1}'
        hour_offset = random.randint(0, 10)  # 8 AM to 6 PM
        timestamp = generate_timestamp('2026-03-22', hour_offset)
        
        # Legitimate users access public pages
        resource = random.choice([r for r in RESOURCES if not any(
            bad in r.lower() for bad in ['admin', 'config', 'shell', 'passwd', 'phpmyadmin', 'cpanel', 'sql']
        )])
        
        method = random.choice(['GET', 'POST']) if resource == '/contact.html' else 'GET'
        status = 201 if method == 'POST' else 200
        size = get_page_size(resource)
        
        records.append({
            'ip': ip,
            'user': user,
            'timestamp': timestamp,
            'method': method,
            'resource': resource,
            'protocol': 'HTTP/1.1',
            'status': status,
            'size': size
        })
        legitimate_count += 1
    
    # Generate bot traffic (10 records)
    print("Generating bot traffic...")
    for i in range(10):
        ip = random.choice(BOT_IPS)
        user = 'bot1'
        hour_offset = random.randint(2, 10)
        timestamp = generate_timestamp('2026-03-22', hour_offset)
        
        # Bot accesses public pages
        resource = random.choice([r for r in RESOURCES if not any(
            bad in r.lower() for bad in ['admin', 'config', 'shell', 'passwd', 'phpmyadmin']
        )])
        
        method = 'GET'
        status = 200
        size = get_page_size(resource)
        
        records.append({
            'ip': ip,
            'user': user,
            'timestamp': timestamp,
            'method': method,
            'resource': resource,
            'protocol': 'HTTP/1.1',
            'status': status,
            'size': size
        })
        bot_count += 1
    
    # Generate attacker traffic (25 records)
    print("Generating attacker traffic...")
    attack_resources = [r for r in RESOURCES if any(
        bad in r.lower() for bad in ['admin', 'config', 'shell', 'passwd', 'phpmyadmin', 'cpanel', 'sql', 'etc', 'inject', 'xmlrpc']
    )]
    
    for i in range(25):
        ip = random.choice(ATTACKER_IPS)
        user = 'hacker1'
        hour_offset = random.randint(2, 10)
        timestamp = generate_timestamp('2026-03-22', hour_offset)
        
        # Attacker tries to access admin/config resources
        resource = random.choice(attack_resources)
        
        method = 'GET'
        status = 403  # All attacker requests are blocked
        size = 0  # No content returned
        
        records.append({
            'ip': ip,
            'user': user,
            'timestamp': timestamp,
            'method': method,
            'resource': resource,
            'protocol': 'HTTP/1.1',
            'status': status,
            'size': size
        })
        attacker_count += 1
    
    # Sort by timestamp
    records.sort(key=lambda x: x['timestamp'])
    
    # Trim to exactly 95 records if needed
    if len(records) > 95:
        records = records[:95]
    
    return records


def write_csv(records, filename):
    """Write records to CSV file"""
    print(f"Writing {len(records)} records to {filename}...")
    
    with open(filename, 'w', newline='') as f:
        fieldnames = ['ip', 'user', 'timestamp', 'method', 'resource', 'protocol', 'status', 'size']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        for record in records:
            writer.writerow(record)
    
    print(f"✅ Successfully created {filename}")
    print(f"   Total records: {len(records)}")
    
    # Print statistics
    legitimate = sum(1 for r in records if r['ip'].startswith('192.168.1'))
    bot = sum(1 for r in records if r['ip'] == '198.51.100.99')
    attacker = sum(1 for r in records if r['ip'] == '203.0.113.45')
    
    print(f"\n📊 Dataset Statistics:")
    print(f"   Legitimate users (192.168.1.x): {legitimate} records")
    print(f"   Bot traffic (198.51.100.99): {bot} records")
    print(f"   Attacker traffic (203.0.113.45): {attacker} records")
    
    success = sum(1 for r in records if r['status'] == 200)
    created = sum(1 for r in records if r['status'] == 201)
    forbidden = sum(1 for r in records if r['status'] == 403)
    
    print(f"\n📈 Status Code Distribution:")
    print(f"   200 (Success): {success} records")
    print(f"   201 (Created): {created} records")
    print(f"   403 (Forbidden): {forbidden} records")


if __name__ == '__main__':
    print("=" * 70)
    print("LOG DATA ANALYSIS - DATASET GENERATOR")
    print("=" * 70)
    print()
    
    # Generate logs
    logs = generate_logs()
    
    # Write to CSV
    write_csv(logs, OUTPUT_FILE)
    
    print()
    print("=" * 70)
    print("✅ Dataset generation complete!")
    print("=" * 70)
