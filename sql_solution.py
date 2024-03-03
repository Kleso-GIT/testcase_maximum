import pandas as pd
from sqlalchemy import create_engine, exc

query = """

WITH ranked_sessions AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY visitor_id ORDER BY date_time) AS row_n
    FROM web_data.sessions
)

SELECT c.communication_id,
       c.site_id,
       c.visitor_id,
       c.date_time AS communication_date_time,
       s.visitor_session_id,
       s.date_time AS sessions_date_time,
       s.campaign_id,
       CAST(s.row_n AS INTEGER) AS row_n
FROM web_data.communications c
LEFT JOIN ranked_sessions s 
    ON c.visitor_id = s.visitor_id 
    AND c.site_id = s.site_id 
    AND s.date_time = (
        SELECT MAX(date_time)
        FROM ranked_sessions
        WHERE visitor_id = c.visitor_id
            AND site_id = c.site_id
            AND date_time <= c.date_time
    )
ORDER BY c.communication_id, s.date_time DESC;
"""


def sql_solution(database_url, print_res=False):
    try:
        engine = create_engine(database_url)
        final_df = pd.read_sql(query, engine)
        if final_df is not None:
            if print_res:
                print(final_df)
        else:
            print("Error merging data. Exiting...")
        return final_df
    except exc.SQLAlchemyError as e:
        print(f"Error reading data from the database: {e}")
        return
