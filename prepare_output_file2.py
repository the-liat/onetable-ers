from datetime import date
from shared import read_dataset
from collections import defaultdict, Counter
from tablib import Dataset
import maya


def main():
    # min_date =
    # max_date =
    cols = ['Start Date', 'First Name', 'Last Name', 'Location', 'unique_id']
    all_guests_all_time = read_dataset('data/all_guests_all_time.csv', cols)

    participant_events = defaultdict(list)

    for r in all_guests_all_time.dict:
        participant_events[r['unique_id']].append(
            dict(first_name=r['First Name'],
                 last_name=r['Last Name'],
                 date=maya.parse(r['Start Date']).datetime().date(),
                 hub=r['Location'])
        )

    ds = ers_dataset = Dataset()
    ds.headers = """id first_name last_name hub dinner_dates 
                    dinners_3m_before dinners_3m_after dinners_6m_before dinners_6m_after""".split()

    users_with_multiple_names = {}
    for unique_id, events in participant_events.items():
        r = [unique_id]

        names = set((e['first_name'], e['last_name']) for e in events)
        if len(names) != 1:
            users_with_multiple_names[unique_id] = names

        hub = Counter(e['hub'] for e in events).most_common(1)[0][0]
        first_name, last_name = names.pop()
        r += [first_name, last_name, hub]
        event_dates = [e['date'] for e in events]
        r.append(','.join(str(ed) for ed in event_dates))

        anchor_date = date(2016, 10, 1)
        date_3m_before = date(2016, 7, 1)
        date_3m_after = date(2016, 12, 31)
        date_6m_before = date(2016, 4, 1)
        date_6m_after = date(2017, 3, 31)

        dinners_3m_before = len([ed for ed in event_dates if date_3m_before < ed <= anchor_date])
        dinners_3m_after = len([ed for ed in event_dates if anchor_date < ed <= date_3m_after])
        dinners_6m_before = len([ed for ed in event_dates if date_6m_before < ed <= anchor_date])
        dinners_6m_after = len([ed for ed in event_dates if anchor_date < ed <= date_6m_after])

        r += [dinners_3m_before, dinners_3m_after, dinners_6m_before, dinners_6m_after]

        ds.append(r)
        print(r)
    open('output2.csv', 'wb').write(ds.csv.encode('utf-8'))
    print('Done.')

if __name__ == '__main__':
    main()
