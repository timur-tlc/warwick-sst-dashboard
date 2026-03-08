"""
Event Sequence Analysis: Compare Event Sequences for Matched Sessions

For matched "Both" sessions, compares the event sequences between
SST and Direct to validate match quality.

High similarity = confident matches
Low similarity = potential false positives
"""

import pandas as pd
import numpy as np
import boto3
from google.cloud import bigquery
import time
from pathlib import Path
from collections import Counter

CACHE_DIR = Path(__file__).parent / "cache"

DATE_START = '20260106'
DATE_END = '20260113'

# Number of matched pairs to analyze (full analysis is expensive)
SAMPLE_SIZE = 100


def load_cached_data():
    """Load session data from cache."""
    sst_df = pd.read_parquet(CACHE_DIR / "sst_sessions.parquet")
    direct_df = pd.read_parquet(CACHE_DIR / "direct_sessions.parquet")
    return sst_df, direct_df


def get_event_sequences_bq(session_ids, limit=500):
    """Query BigQuery for event sequences of specific sessions."""
    if not session_ids:
        return {}

    sample_ids = session_ids[:limit]
    ids_clause = ','.join([f"'{sid}'" for sid in sample_ids])

    bq_client = bigquery.Client(project="376132452327")

    query = f"""
    SELECT
        CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) as ga_session_id,
        event_name,
        event_timestamp
    FROM `analytics_375839889.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '{DATE_START}' AND '{DATE_END}'
      AND CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) IN ({ids_clause})
    ORDER BY ga_session_id, event_timestamp
    """

    df = bq_client.query(query).to_dataframe()

    # Group by session_id
    sequences = {}
    for session_id, group in df.groupby('ga_session_id'):
        sequences[session_id] = group['event_name'].tolist()

    return sequences


def get_event_sequences_athena(session_ids, limit=500):
    """Query Athena for event sequences of specific sessions."""
    if not session_ids:
        return {}

    sample_ids = session_ids[:limit]
    ids_clause = ','.join([f"'{sid}'" for sid in sample_ids])

    session = boto3.Session(profile_name='warwick')
    athena = session.client('athena', region_name='ap-southeast-2')

    year = DATE_START[:4]
    month = DATE_START[4:6]
    day_start = DATE_START[6:8]
    day_end = DATE_END[6:8]

    query = f"""
    SELECT
        ga_session_id,
        event_name,
        timestamp
    FROM warwick_weave_sst_events.sst_events_transformed
    WHERE site = 'AU'
      AND year = '{year}'
      AND month = '{month}'
      AND day BETWEEN '{day_start}' AND '{day_end}'
      AND ga_session_id IN ({ids_clause})
    ORDER BY ga_session_id, timestamp
    """

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'warwick_weave_sst_events'},
        ResultConfiguration={'OutputLocation': 's3://warwick-com-au-athena-results/'},
        WorkGroup='primary'
    )
    query_id = response['QueryExecutionId']

    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1)

    if state != 'SUCCEEDED':
        raise Exception("Athena query failed")

    rows = []
    next_token = None
    while True:
        if next_token:
            results = athena.get_query_results(QueryExecutionId=query_id, NextToken=next_token, MaxResults=1000)
        else:
            results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1000)

        result_rows = results['ResultSet']['Rows']
        if not next_token:
            result_rows = result_rows[1:]

        for row in result_rows:
            rows.append([field.get('VarCharValue', '') for field in row['Data']])

        next_token = results.get('NextToken')
        if not next_token:
            break

    df = pd.DataFrame(rows, columns=['ga_session_id', 'event_name', 'timestamp'])

    sequences = {}
    for session_id, group in df.groupby('ga_session_id'):
        sequences[session_id] = group['event_name'].tolist()

    return sequences


def sequence_similarity(seq1, seq2):
    """
    Calculate similarity between two event sequences.

    Uses multiple metrics:
    1. Jaccard similarity (set overlap)
    2. Event count match
    3. First event match
    """
    if not seq1 or not seq2:
        return {'jaccard': 0, 'count_diff': abs(len(seq1) - len(seq2)), 'first_match': False}

    # Jaccard similarity
    set1 = set(seq1)
    set2 = set(seq2)
    jaccard = len(set1 & set2) / len(set1 | set2) if set1 | set2 else 0

    # Count-weighted similarity (how many of each event type)
    counter1 = Counter(seq1)
    counter2 = Counter(seq2)
    all_events = set(counter1.keys()) | set(counter2.keys())
    total_match = sum(min(counter1.get(e, 0), counter2.get(e, 0)) for e in all_events)
    total_events = sum(counter1.values()) + sum(counter2.values())
    count_similarity = 2 * total_match / total_events if total_events > 0 else 0

    # First event match
    first_match = seq1[0] == seq2[0] if seq1 and seq2 else False

    # Length difference
    length_diff = abs(len(seq1) - len(seq2))

    return {
        'jaccard': jaccard,
        'count_similarity': count_similarity,
        'first_match': first_match,
        'length_diff': length_diff,
        'sst_events': len(seq1),
        'direct_events': len(seq2)
    }


def main():
    print("=" * 70)
    print("EVENT SEQUENCE ANALYSIS: Match Quality Validation")
    print("=" * 70)

    sst_df, direct_df = load_cached_data()

    # Get "Both" sessions
    both_sst = sst_df[sst_df['session_category'] == 'Both'].copy()
    both_direct = direct_df[direct_df['session_category'] == 'Both'].copy()

    print(f"\nMatched 'Both' sessions: {len(both_sst):,}")

    if len(both_sst) == 0:
        print("No 'Both' category sessions found. Cannot perform sequence analysis.")
        return

    # Sample for analysis
    np.random.seed(42)
    sample_sst = both_sst.sample(n=min(SAMPLE_SIZE, len(both_sst)))
    sample_sst_ids = sample_sst['ga_session_id'].tolist()

    print(f"Analyzing {len(sample_sst_ids)} session pairs...")

    # For each SST session, find the matching Direct session
    # Re-match to find pairs (since we don't store the pairing)
    time_window_micros = 300 * 1000000

    matched_pairs = []
    for _, sst_row in sample_sst.iterrows():
        sst_ts = sst_row['session_start_ts']

        candidates = both_direct[
            (both_direct['session_start_ts'] >= sst_ts - time_window_micros) &
            (both_direct['session_start_ts'] <= sst_ts + time_window_micros) &
            (both_direct['device_category'] == sst_row['device_category']) &
            (both_direct['geo_country'] == sst_row['geo_country'])
        ]

        if len(candidates) > 0:
            candidates = candidates.copy()
            candidates['ts_diff'] = abs(candidates['session_start_ts'] - sst_ts)
            best_match = candidates.nsmallest(1, 'ts_diff').iloc[0]
            matched_pairs.append({
                'sst_id': sst_row['ga_session_id'],
                'direct_id': best_match['ga_session_id']
            })

    print(f"Found {len(matched_pairs)} matched pairs for sequence analysis")

    if len(matched_pairs) == 0:
        print("No matched pairs found for analysis.")
        return

    # Get event sequences from both sources
    print("\nQuerying event sequences from BigQuery...")
    direct_ids = [p['direct_id'] for p in matched_pairs]
    direct_sequences = get_event_sequences_bq(direct_ids)
    print(f"  Retrieved sequences for {len(direct_sequences)} Direct sessions")

    print("Querying event sequences from Athena...")
    sst_ids = [p['sst_id'] for p in matched_pairs]
    sst_sequences = get_event_sequences_athena(sst_ids)
    print(f"  Retrieved sequences for {len(sst_sequences)} SST sessions")

    # Compare sequences
    print("\nComparing event sequences...")

    similarities = []
    for pair in matched_pairs:
        sst_seq = sst_sequences.get(pair['sst_id'], [])
        direct_seq = direct_sequences.get(pair['direct_id'], [])

        if sst_seq and direct_seq:
            sim = sequence_similarity(sst_seq, direct_seq)
            sim['sst_id'] = pair['sst_id']
            sim['direct_id'] = pair['direct_id']
            similarities.append(sim)

    if not similarities:
        print("No sequences could be compared.")
        return

    sim_df = pd.DataFrame(similarities)

    # Report results
    print("\n" + "-" * 70)
    print("SEQUENCE SIMILARITY STATISTICS")
    print("-" * 70)

    print(f"\nPairs analyzed: {len(sim_df)}")

    print(f"\nJaccard Similarity (set overlap):")
    print(f"  Mean:   {sim_df['jaccard'].mean():.3f}")
    print(f"  Median: {sim_df['jaccard'].median():.3f}")
    print(f"  Std:    {sim_df['jaccard'].std():.3f}")

    print(f"\nCount-weighted Similarity:")
    print(f"  Mean:   {sim_df['count_similarity'].mean():.3f}")
    print(f"  Median: {sim_df['count_similarity'].median():.3f}")
    print(f"  Std:    {sim_df['count_similarity'].std():.3f}")

    print(f"\nFirst Event Match Rate:")
    first_match_rate = sim_df['first_match'].mean() * 100
    print(f"  {first_match_rate:.1f}% of matched pairs have the same first event")

    print(f"\nEvent Count Difference:")
    print(f"  Mean:   {sim_df['length_diff'].mean():.1f} events")
    print(f"  Median: {sim_df['length_diff'].median():.1f} events")
    print(f"  Max:    {sim_df['length_diff'].max():.0f} events")

    # Distribution of similarity scores
    print("\n" + "-" * 70)
    print("SIMILARITY DISTRIBUTION")
    print("-" * 70)

    print(f"\nJaccard Similarity Buckets:")
    buckets = [(0, 0.2, 'Very Low'), (0.2, 0.4, 'Low'), (0.4, 0.6, 'Medium'),
               (0.6, 0.8, 'High'), (0.8, 1.0, 'Very High'), (1.0, 1.01, 'Perfect')]

    for low, high, label in buckets:
        count = len(sim_df[(sim_df['jaccard'] >= low) & (sim_df['jaccard'] < high)])
        pct = count / len(sim_df) * 100
        print(f"  {label:<12} ({low:.1f}-{high:.1f}): {count:>4} ({pct:>5.1f}%)")

    # Identify problematic matches
    print("\n" + "-" * 70)
    print("POTENTIAL FALSE POSITIVES (Low Similarity)")
    print("-" * 70)

    low_sim = sim_df[sim_df['jaccard'] < 0.3]
    print(f"\nPairs with Jaccard < 0.3: {len(low_sim)} ({len(low_sim)/len(sim_df)*100:.1f}%)")

    if len(low_sim) > 0:
        print("\nSample of low-similarity pairs:")
        for _, row in low_sim.head(5).iterrows():
            print(f"\n  SST ID: {row['sst_id']}")
            print(f"  Direct ID: {row['direct_id']}")
            print(f"  Jaccard: {row['jaccard']:.3f}, Events: {row['sst_events']} vs {row['direct_events']}")

    # Save results
    sim_df.to_csv('docs/event_sequence_similarity.csv', index=False)
    print(f"\nResults saved to docs/event_sequence_similarity.csv")

    # Verdict
    print("\n" + "=" * 70)
    print("VERDICT")
    print("=" * 70)

    avg_jaccard = sim_df['jaccard'].mean()
    high_sim_pct = len(sim_df[sim_df['jaccard'] >= 0.6]) / len(sim_df) * 100

    if avg_jaccard >= 0.6 and high_sim_pct >= 70:
        print(f"\nHIGH CONFIDENCE: {high_sim_pct:.1f}% of pairs have Jaccard >= 0.6")
        print("Event sequences are highly similar between matched SST/Direct sessions.")
        print("This validates the matching algorithm is finding true matches.")
    elif avg_jaccard >= 0.4:
        print(f"\nMODERATE CONFIDENCE: Average Jaccard = {avg_jaccard:.3f}")
        print("Most matches appear valid, but some may be false positives.")
        print(f"  High similarity (>=0.6): {high_sim_pct:.1f}%")
        print(f"  Low similarity (<0.3):  {len(low_sim)/len(sim_df)*100:.1f}%")
    else:
        print(f"\nLOW CONFIDENCE: Average Jaccard = {avg_jaccard:.3f}")
        print("Many matched pairs have dissimilar event sequences.")
        print("Consider tightening matching criteria or investigating discrepancies.")

    # Explanation for discrepancies
    print("\n" + "-" * 70)
    print("NOTES ON SEQUENCE DIFFERENCES")
    print("-" * 70)
    print("""
Some difference between SST and Direct sequences is EXPECTED because:
1. Timing differences: Events near session boundaries may differ
2. Event loss: Ad-blockers may block some Direct events
3. Server-side enrichment: SST may include additional server events
4. Client-side only events: Some events only fire client-side

A Jaccard of 0.6-0.8 is generally acceptable for this use case.
""")


if __name__ == "__main__":
    main()
