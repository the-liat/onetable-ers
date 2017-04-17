from datetime import date
from .shared import read_dataset
from collections import defaultdict, Counter

import maya

ot_employees = [
    ('dannyjcohen@gmail.com', 'Danny', 'Cohen'),
    ('10154564535279867@facebook.com', 'Aliza', 'Kline'),
]

ot_employee_emails = set(e[0] for e in ot_employees)




def main():
    # min_date =
    # max_date =

    profiles = read_dataset('data/profiles.csv', 'id user_id first_name last_name'.split())
    reservations = read_dataset('data/reservations.csv', ('profile_id', 'event_id', ))
    user_dict = read_dataset('data/users.csv', ('id', 'email'), as_dict=True)
    event_ds = read_dataset('data/events.csv', ('id',
                                                'event_date',
                                                'area_code',
                                                'reservations_total',
                                                'price_per_person',
                                                'event_type'))
    unreserved_event_indices = [i for i, e in enumerate(event_ds.dict) if e['reservations_total'] in ('0', '')]
    for i in sorted(unreserved_event_indices, reverse=True):
        del event_ds[i]

    event_dict = {e['id']: dict(date=maya.parse(e['event_date']).datetime().date(),
                                hub=e['area_code'],
                                price=str(int(e['price_per_person']) // 100) if e['price_per_person'] not in ('', '0') else '',
                                type=e['event_type']) for e in event_ds.dict}

    participant_events = defaultdict(list)
    bad_reservations_no_profile_id = []
    bad_reservations_no_event_id = []
    profiles_without_events = []
    bad_events_no_hub = []
    for r in reservations.dict:
        if not r['profile_id']:
            bad_reservations_no_profile_id.append(r)
            continue
        event_id = r['event_id']
        if not event_id:
            bad_reservations_no_event_id.append(r)
            continue
        event = event_dict[event_id]
        if not event['hub']:
            bad_events_no_hub.append(event_id)
        participant_events[r['profile_id']].append(event)

    ds = ers_dataset = Dataset()
    ds.headers = """id email first_name last_name hub dinner_dates 
                    dinners_3m_before dinners_3m_after dinners_6m_before dinners_6m_after
                    
                    event_prices
                    
                    paid_dinners_3m_before 
                    paid_dinners_3m_after 
                    paid_dinners_6m_before 
                    paid_dinners_6m_after

                    event_types

                    paid_private_dinners_3m_before 
                    paid_private_dinners_3m_after 
                    paid_private_dinners_6m_before 
                    paid_private_dinners_6m_after""".split()

    for p in profiles.dict:
        if p['user_id'] not in user_dict:
            continue

        email = user_dict[p['user_id']]
        if email in ot_employee_emails:
            continue

        r = [p['id'], email]
        r += [p['first_name'], p['last_name']]
        profile_id = p['id']
        if profile_id not in participant_events:
            profiles_without_events.append(p)
            continue
        events = participant_events[profile_id]
        hubs = [e['hub'] for e in events]
        c = Counter(hubs)
        hub = c.most_common(1)[0][0]
        r.append(hub)
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

        event_prices = [e['price'] for e in events]
        r.append(','.join(str(ep) for ep in event_prices))

        paid_dinners_3m_before = len([e for e in events if (date_3m_before < e['date'] <= anchor_date) and e['price']])
        paid_dinners_3m_after = len([e for e in events if (anchor_date < e['date'] <= date_3m_after) and e['price']])
        paid_dinners_6m_before = len([e for e in events if (date_6m_before < e['date'] <= anchor_date) and e['price']])
        paid_dinners_6m_after = len([e for e in events if (anchor_date < e['date'] <= date_6m_after) and e['price']])

        r += [paid_dinners_3m_before,
              paid_dinners_3m_after,
              paid_dinners_6m_before,
              paid_dinners_6m_after]

        paid_private_dinners_3m_before = len([e for e in events if (date_3m_before < e['date'] <= anchor_date)
                                              and e['price'] and e['type'] != 'public'])
        paid_private_dinners_3m_after = len([e for e in events if (anchor_date < e['date'] <= date_3m_after)
                                             and e['price'] and e['type'] != 'public'])
        paid_private_dinners_6m_before = len([e for e in events if (date_6m_before < e['date'] <= anchor_date)
                                              and e['price'] and e['type'] != 'public'])
        paid_private_dinners_6m_after = len([e for e in events if (anchor_date < e['date'] <= date_6m_after)
                                             and e['price'] and e['type'] != 'public'])


        event_types = [e['type'] for e in events]
        r.append(','.join(str(et) for et in event_types))

        r += [paid_private_dinners_3m_before,
              paid_private_dinners_3m_after,
              paid_private_dinners_6m_before,
              paid_private_dinners_6m_after]

        ds.append(r)
        print(r)
    open('output1.csv', 'wb').write(ds.csv.encode('utf-8'))
    print('Done.')

if __name__ == '__main__':
    main()
