from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt

bat_performance = "Datasets\men_odi_team_batting_stats.csv"
stadium_info = "Datasets\men_odi_team_match_results_21st_century.csv"

spark = SparkSession.builder.appName(name='Cricket Analysis').getOrCreate()

df_odi = spark.read.csv(path=bat_performance, header=True, inferSchema=True)
df_stadium = spark.read.csv(path=stadium_info,header=True,inferSchema=True)

df_odi = df_odi.withColumn(

    'Total_Matches', df_odi.matches_won + df_odi.matches_lost + df_odi.matches_tied + df_odi.matches_with_no_result
)

df_odi = df_odi.withColumn('Win_Rate',df_odi.matches_won/df_odi.Total_Matches)

# df_odi.show()
# df_odi = df_odi.withColumn('country',col('COUNTry'))

df_final_odi=df_stadium.join(df_odi,df_stadium.country == df_odi.country,"left").filter(df_stadium.first_batting_won=='Yes')

# df_final_odi.show()

df_final_odi=df_final_odi.select(col('ground'), df_final_odi.Win_Rate*100).dropDuplicates()

rows= df_final_odi.count()

# print(rows)

df_final_odi.show()

# df_odi.show()

# plt.plot(df_final_odi['ground'],df_final_odi.Win_Rate)
# plt.show()

# df_final_odi.describe()
"""

Schema : Stadium Name | Won Matches | Total Matches | First Toss

group by Stadium

"""



