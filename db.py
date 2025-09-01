import duckdb
import json
import tempfile
import os

class Database:
    def __init__(self, database_name="mydb.duckdb"):
        # Delete the database file for this test script
        if os.path.exists(database_name):
            os.remove(database_name)
        self.con = duckdb.connect(database=":memory:")

    def load_csv(self, csv_file_path="amz_uk_processed_data.csv", table_name="products"):
        self.con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file_path}')")

    def group_by_category_stats(self, table_name="products"):
        group_by_category_stats = self.con.execute(f"SELECT categoryName, AVG(stars) as avg_rating, STDDEV(stars) as std_dev_rating, VARIANCE(stars) as var_rating FROM {table_name} GROUP BY categoryName ORDER BY VARIANCE(stars) DESC")
        group_by_category_stats_df = group_by_category_stats.fetch_df()
        # print(group_by_category_stats_df.head())
        return group_by_category_stats_df
    
    def get_siginificant_categories(self, group_by_category_stats_df):
        
        high_variance_categories = group_by_category_stats_df.head(5)[['categoryName', 'var_rating']]
        # if we want to return categories only without variance values, that could be done like below
        # high_variance_categories = group_by_category_stats_df.head(5)['categoryName']
        low_variance_categories = group_by_category_stats_df.tail(5)[['categoryName', 'var_rating']]
        return high_variance_categories, low_variance_categories
        
    def get_z_score(self, group_by_category_stats_df):
        z_scores = self.con.execute("""
                WITH stats AS (
                    SELECT categoryName,
                        AVG(stars) AS avg_rating,
                        STDDEV(stars) AS stddev_rating,
                        COUNT(*) AS n
                    FROM products
                    GROUP BY categoryName
                ),
                global_stats AS (
                    SELECT AVG(stars) AS global_mean,
                        STDDEV(stars) AS global_std
                    FROM products
                )
                SELECT s.categoryName,
                    s.avg_rating,
                    (s.avg_rating - g.global_mean) / g.global_std AS z_score
                FROM stats s, global_stats g
                ORDER BY ABS(z_score) DESC
            """).df()
        return z_scores

    def analysis_pipeline(self):
        self.load_csv()
        row_count = (self.con.execute("SELECT COUNT(*) FROM products").fetchone()[0])
        print(f"Loaded {row_count} rows into the database")

        # Group by category stats
        # Average rating
        # Standard deviation of ratings
        # Variance of ratings
        print("--------------------------------")
        print("\nGrouping by category stats - Avg rating, Std dev rating, Var rating")
        print("--------------------------------")
        group_by_category_stats_df = self.group_by_category_stats()
        print(group_by_category_stats_df.head())

        # Get significant categories with high/low (siginificant) variability in ratings
        print("\nGetting significant categories with high/low variability in ratings")
        high_variance_categories, low_variance_categories = self.get_siginificant_categories(group_by_category_stats_df)
        print("--------------------------------")
        print("\nHigh variance categories")
        print("--------------------------------")
        print(high_variance_categories)
        print("--------------------------------")
        print("\nLow variance categories")
        print("--------------------------------")
        print(low_variance_categories)

        # Using z-score to compare high/low ratings against global average
        print("Using z-score to compare high/low ratings against global average")
        z_scores = self.get_z_score(group_by_category_stats_df)
        # print(z_scores)
        # print top 5 categories with highest z-scores  
        print("--------------------------------")
        print("\nTop 5 categories with highest z-scores")
        print("--------------------------------")
        print(z_scores.head(5))
        # print top 5 categories with lowest z-scores
        print("--------------------------------")
        print("\nTop 5 categories with lowest z-scores")
        print("--------------------------------")
        print(z_scores.tail(5))

if __name__ == '__main__':

    db = Database()
    db.analysis_pipeline()
    # print(db.query("SELECT * FROM products"))
    # db.upsert_duckdb(data2, table_name="products")
    # print(db.get_records_count("products"))
    # db.head("products")
