import psycopg2

class DimensionEnrichment:
    def __init__(self, jdbc_url: str, user: str, pwd: str):
        self.conn = psycopg2.connect(jdbc_url, user=user, password=pwd)
        self.cur  = self.conn.cursor()
        
    def upsert_and_get_sk(
        self,
        table: str,
        business_key: tuple[str],   
        columns: dict            
    ) -> int:
        cols = ', '.join(columns.keys())
        vals = tuple(columns.values())
        placeholders = ', '.join(['%s'] * len(vals))
        conflict_cols = ', '.join(business_key)
        set_clause = ', '.join(f"{c} = EXCLUDED.{c}" for c in columns.keys())
        sql = f"""
            INSERT INTO {table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_cols})
            DO UPDATE SET {set_clause}
            RETURNING {table.split('_')[-1]}_sk
        """
        self.cur.execute(sql, vals)
        sk = self.cur.fetchone()[0]
        self.conn.commit()
        return sk
