#!/usr/bin/env python3

import gzip
import os
import re
import sys

import psycopg2


def write_plan(plan, db, output, plan_counter):
    output_dir = os.path.join('plans', db)
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(output_dir, f'{output}_{plan_counter}.json')
    with open(output_path, 'w') as output_file:
        output_file.write(plan)
        print(f'Wrote plan to {output_path}')

def write_to_db(plan, db, timestamp):
    try:
        query_name = re.search('_Result[0-9]+', plan)[0]
    except (TypeError, IndexError):
        query_name = timestamp

    date, time_ = timestamp.split('_')
    time_ = time_.replace('-', ':')
    conn = psycopg2.connect('postgresql://postgres@si-prod-002.swarm64.com/query_analysis')
    conn.autocommit = True
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'''
                INSERT INTO varicent_query_plans VALUES(
                    '{db}'
                  , '{date} {time_}'
                  , '{query_name}'
                  , $${plan}$$
                )''')

    except psycopg2.errors.UniqueViolation:
        print('Plan already exists in DB')

    finally:
        conn.close()

if __name__ == '__main__':
    INPUT = sys.argv[1]

    log_content = None
    with gzip.open(INPUT, 'r') as handle:
        try:
            log_content = handle.read()
            log_content = log_content.decode('utf-8')
        except OSError:
            print('Not a GZIP file, assuming text')

    if not log_content:
        with open(INPUT, 'r') as log_file:
            log_content = log_file.read()

    assert log_content, 'Log file empty?'

    plan = ''
    has_plan = False
    plan_counter = 1
    db = ''
    timestamp = ''
    for line in log_content.split('\n'):
        if has_plan and re.match('^[0-9]{4}-[0-9]{2}-[0-9]{2}', line):
            has_plan = False
            print('Plan end')

            # write_plan(plan, db[3:], timestamp, plan_counter)
            write_to_db(plan, db[3:], timestamp)
            plan_counter += 1

            plan = ''

        if 'plan:' in line:
            date, time_, tz, pid, conn, level, _, duration, _, _  = [
                item for item in line.split(' ') if item]
            db, user, app, client = conn.split(',')
            timestamp = f'{date}_{time_}'.replace(':','-')

            has_plan = True
            print('Plan begin')
            continue

        if has_plan:
            plan += line
