import os

from neo4j import GraphDatabase
import yaml
from dotenv import load_dotenv
from catalog import dataProduct,Dataset,Column,Row
from EDGAR_2024_GHG import EDGAR_GHG_2024

load_dotenv()

file = 'EDGAR_GHG_2024_per_capita_by_country'
testset = EDGAR_GHG_2024(file)
cypher = testset.getNeo4jrepresentation()

with GraphDatabase.driver(os.getenv("GraphEndpoint"),
                          auth=(os.getenv("GraphUser"),os.getenv("GraphPassword"))) as driver:
    driver.verify_connectivity()

    records, summary, keys = driver.execute_query(
        cypher,database_="neo4j",
    )

    # Loop through results and do something with them
    for person in records:
        print(person)

    # Summary information
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query=summary.query, records_count=len(records),
        time=summary.result_available_after,
    ))


