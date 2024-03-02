import pandas as pd
from sqlalchemy import create_engine, exc


def read_data(database_url):
    try:
        engine = create_engine(database_url)
        communications_df = pd.read_sql("select * from web_data.communications", engine)
        sessions_df = pd.read_sql("select * from web_data.sessions", engine)
        return communications_df, sessions_df
    except exc.SQLAlchemyError as e:
        print(f"Error reading data from the database: {e}")
        return None, None


def preprocess_data(communications_df, sessions_df):
    try:
        communications_df = communications_df.rename(columns={'date_time': 'communication_date_time'})
        sessions_df = sessions_df.rename(columns={'date_time': 'sessions_date_time'})
        sessions_df = sessions_df.sort_values(by=['sessions_date_time', 'visitor_id'])
        sessions_df['row_n'] = sessions_df.groupby('visitor_id').cumcount() + 1
        return communications_df, sessions_df
    except KeyError as e:
        print(f"Error in preprocessing data: {e}")
        return None, None


def find_closest_session(group):
    try:
        if not group.empty:
            return group.loc[(group['sessions_date_time'] - group['communication_date_time']).abs().idxmin()]
        else:
            return None
    except KeyError as e:
        print(f"Error in finding closest session: {e}")
        return None


def merge_data(communications_df, sessions_df):
    try:
        merged_data = pd.merge(communications_df, sessions_df, on=['visitor_id', 'site_id'], how='left')
        closest_sessions = merged_data.groupby('communication_id', as_index=False).apply(find_closest_session)
        closest_sessions = closest_sessions.reset_index(drop=True)
        closest_sessions = closest_sessions[
            ['communication_id', 'visitor_session_id', 'campaign_id', 'sessions_date_time', 'row_n']]
        final_data = pd.merge(communications_df, closest_sessions, on='communication_id', how='left')
        return final_data
    except pd.errors.MergeError as e:
        print(f"Error in merging data: {e}")
        return None


def pandas_solution(database_url, print_res=False):
    communications_df, sessions_df = read_data(database_url)
    if communications_df is None or sessions_df is None:
        print("Error loading data. Exiting...")
        return
    communications_df, sessions_df = preprocess_data(communications_df, sessions_df)
    if communications_df is None or sessions_df is None:
        print("Error preprocessing data. Exiting...")
        return
    final_data = merge_data(communications_df, sessions_df)
    if final_data is not None:
        if print_res:
            print(final_data)
        return final_data
    else:
        print("Error merging data. Exiting...")