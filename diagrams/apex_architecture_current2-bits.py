from diagrams import Diagram, Cluster, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.analytics import ManagedStreamingForKafka
from diagrams.aws.storage import S3
from diagrams.aws.analytics import Glue
from diagrams.aws.ml import Sagemaker
from diagrams.onprem.database import Postgresql, Mongodb
from diagrams.saas.analytics import Snowflake
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.compute import Server
from diagrams.generic.device import Mobile
from functools import partial

# define your custom objects
Edge = partial(Edge, fontsize="20")
# Cluster = partial(Cluster, fontsize="28")

# Set diagram attributes
graph_attr = {
    "fontsize": "28",
    "bgcolor": "white",
    "splines": "polyline",  # Better edge routing for alignment
    "nodesep": "1.2",  # Horizontal node spacing
    "ranksep": "2.0",  # Increased rank spacing
    "pad": "0.5"
}

node_attr = {
    "fontsize": "20",
    # "width": "1.8",
    # "height": "1.0",  # Reduced height for tighter spacing
    "fixedsize": "true",
    "imagescale": "true"
}

cluster_attr = {
    "fontsize": "24",
    "style": "rounded"
    # "margin": "20"
}

# edge_attr = {
#     "fontsize": "50pt",
#     "minlen": "1"
# }


with Diagram(
    # "Apex Digital - Current State: The Split Brain Problem",
    filename="apex_architecture_current2-bits",
    show=False,
    direction="LR",
    graph_attr=graph_attr,
    node_attr=node_attr
    # edge_attr=edge_attr
):

    # --- SOURCE SYSTEMS ---
    with Cluster("Transaction Sources", graph_attr={**cluster_attr, "bgcolor": "#e6f4e4"}):
        mobile = Mobile("\nMobile App / P2P")
        core = Server("\nTemenos (Core)")
        fiserv = Server("\nFiserv (Card)")

    # # --- OPERATIONAL SILOS (THE HOT PATH) ---
    # with Cluster(
    #     "HOT PATH: Real-Time Decisions (<1 sec)\nPROBLEM: Siloed State + No ML Features", 
    #     graph_attr={**cluster_attr, "bgcolor": "#f8dec7"}
    # ):
    #     # The traffic cop
    #     api_gw = Lambda("\nAPI GW / \nPayment Switch")

    #     # The fragmented state stores (SILO #3 and #4)
    #     mongo = Mongodb("\nMongoDB\n(Session/Device)\n[SILO #4]")
    #     postgres = Postgresql("\nPostgres\n(Customer Profile)\n[SILO #3]")

    #     # The Legacy Decision Engine
    #     falcon = Server("\nFICO Falcon\n(Rules Engine)\n1:8 False Positive")

    # # --- ANALYTICAL SWAMP (THE COLD PATH) ---
    # with Cluster(
    #     "COLD PATH: Analytics & ML (Hours-Days Latency)\nPROBLEM: Stale Data + Weekly Model Updates",
    #     graph_attr={**cluster_attr, "bgcolor": "#d4f0f9"}
    # ):
    #     kafka = ManagedStreamingForKafka("\nKafka\n(Events)")
    #     s3 = S3("\nS3 Data Lake\n('The Swamp')\n[SILO #1]")
    #     glue = Glue("\nAWS Glue\n(Batch ETL)\n[Spaghetti 1/4]")

    #     # The Reporting Silo
    #     snowflake = Snowflake("\nSnowflake\n(Nightly Batch)\n[SILO #2]")
    #     airflow = Airflow("\nAirflow\n(Orchestrator)\n[Spaghetti 2/4]")

    #     # The Isolated ML
    #     sagemaker = Sagemaker("\nSageMaker\n(Weekly Training)\nOnline/Offline Skew")

    # # --- CONNECTIONS ---

    # # 1. Source Flows (scattered integration patterns)
    # mobile >> Edge(label="Events (Stream)") >> kafka
    # mobile >> Edge(label="Txn API (Real-time)") >> api_gw

    # # Core Banking: The "Two Speed" problem
    # core >> Edge(label="Sync (CDC)", color="green") >> postgres
    # core >> Edge(label="15-min Batch File\n⏱️ LATENCY", color="orange") >> s3

    # # Card Network (Fiserv)
    # fiserv >> Edge(label="Auth Webhook (Real-time)") >> api_gw

    # # 2. The Operational "Hot" Path (The Struggle)
    # # The API GW tries to fetch state from silos
    # api_gw >> Edge(label="Read Session (Silo #4)") >> mongo
    # api_gw >> Edge(label="Read Balance (Silo #3)") >> postgres

    # # The Decision (Logic Gap)
    # api_gw >> Edge(label="Decision Request\n(Rules-Based)", color="darkorange") >> falcon

    # # ⚠️ THE ARROW THAT KILLS ⚠️
    # # The "Missing" Real-time ML Feature Lookup (The Split Brain)
    # api_gw >> Edge(
    #     label="❌ NO REAL-TIME FEATURE LOOKUP ❌\n(45-min blind spot | Online/Offline Skew)",
    #     style="dotted",
    #     color="red",
    #     penwidth="5"
    # ) >> sagemaker

    # # 3. The Analytical "Cold" Path (High Latency)
    # kafka >> Edge(label="Stream → Batch\n⏱️ LAG", color="blue") >> s3
    # api_gw >> Edge(label="API Logs\n(Async)", style="dashed") >> s3

    # # The ETL Slog (Spaghetti Tax)
    # s3 >> Edge(label="Batch ETL\n(Glue Jobs)") >> glue >> Edge(label="Weekly\n⏱️ STALE") >> sagemaker
    # s3 >> Edge(label="Nightly Copy\n⏱️ 24hr OLD") >> snowflake

    # # The "Dead End" (Mongo doesn't feed Snowflake - missing data)
    # mongo >> Edge(label="❌ No Schema\n(Data Missing)", style="dotted", color="red") >> snowflake

    # # 4. The Legacy Feedback Loop (Stale Data Problem)
    # # Snowflake aggregates Postgres but misses MongoDB
    # postgres >> Edge(label="Nightly Sync\n⏱️ 24hr") >> snowflake
    # snowflake >> Edge(label="Aggregates\n(Airflow)") >> airflow >> Edge(label="Daily CSV\n⏱️ STALE", color="orange") >> falcon

    # # SageMaker feeds Falcon (Static Rules - Weekly Update Cycle)
    # sagemaker >> Edge(label="Weekly Rules Push\n⏱️ SLOW ITERATION", color="darkorange") >> falcon