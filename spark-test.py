from pyspark import SparkContext
from pyspark.sql import SparkSession

def main():
    sc = SparkContext()
    spark = SparkSession(sc)

    # Read Input Data
    matches_df = spark.read.csv('sample_data/matches/', header=True, inferSchema=True)
    deliveries_df = spark.read.csv('sample_data/deliveries/', header=True, inferSchema=True)

    #print(matches_df.dtypes)
    #print(matches_df.schema)

    #print(deliveries_df.dtypes)

    matches_df.registerTempTable('matches')
    deliveries_df.registerTempTable('deliveries')

    sql_stmt = '''
        SELECT 
            season, count(id) 
        FROM 
            matches 
        GROUP BY 
            season 
        ORDER BY season
    '''

    """
    sql_stmt = '''
        SELECT 
            d.batting_team, m.date, sum(total_runs) 
        FROM 
            deliveries d
            JOIN matches m ON d.match_id = m.id   
        GROUP BY 
            d.match_id, 
            d.batting_team,
            m.date 
        ORDER BY 3 DESC
        LIMIT 5
    '''
    """

    output_df = spark.sql(sql_stmt)

    # Write Data
    output_df.write.json('sample_data/output/')

if __name__ == '__main__':
    main()