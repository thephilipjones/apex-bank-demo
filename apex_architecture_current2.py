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
    filename="apex_architecture_current2",
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

    # --- OPERATIONAL SILOS (THE HOT PATH) ---
    with Cluster(
        "Real-Time Decisions (<1 sec)\nSiloed Data + Missing ML Features", 
        graph_attr={**cluster_attr, "bgcolor": "#f8dec7"}
    ):
        # The traffic cop
        api_gw = Lambda("\nAPI Gateway + \nPayment Switch")

        # The fragmented state stores (SILO #3 and #4)
        mongo = Mongodb("\nMongoDB\n(Session/Device)\n[Silo]")
        postgres = Postgresql("\nPostgres\n(Customer/Balance)\n[Silo]")

        # The Legacy Decision Engine
        falcon = Server("\nFICO Falcon\nFraud Detection\n(Rule-based, not ML)")

    # --- ANALYTICAL SWAMP (THE COLD PATH) ---
    with Cluster(
        "Slow Analytics & ML\nStale Data + Weekly Model Updates",
        graph_attr={**cluster_attr, "bgcolor": "#d4f0f9"}
    ):
        kafka = ManagedStreamingForKafka("\nKafka\n(Events)")
        s3 = S3("\nS3 Data Lake\n('The Swamp')\n[Silo]")
        glue = Glue("\nAWS Glue\n(Batch ETL)\n[Spaghetti]")

        # The Reporting Silo
        snowflake = Snowflake("\nSnowflake\n(Nightly Batch)\n[Silo]")
        airflow = Airflow("\nAirflow\n(Orchestrator)\n[Spaghetti]")

        # The Isolated ML
        sagemaker = Sagemaker("\n\nSageMaker\n⏱️ Weekly Training\n⛓️‍💥 Skew")

    # --- CONNECTIONS ---

    # 1. Source Flows (scattered integration patterns)
    mobile >> Edge(label="Events (Stream)") >> kafka
    mobile >> Edge(label="Txn API (Real-time)") >> api_gw

    # Core Banking: The "Two Speed" problem
    core >> Edge(label="Sync (CDC)", color="green") >> postgres
    core >> Edge(label="⏱️ 15-min Batch File", color="orange") >> s3

    # Card Network (Fiserv)
    fiserv >> Edge(label="Auth Webhook (Real-time)") >> api_gw

    # 2. The Operational "Hot" Path (The Struggle)
    # The API GW tries to fetch state from silos
    api_gw >> Edge(label="Session/Device data [Silo]") >> mongo
    api_gw >> Edge(label="Balance data [Silo]") >> postgres

    # The Decision (Logic Gap)
    api_gw >> Edge(label="Fraud evaluation", color="darkorange") >> falcon

    # ⚠️ THE ARROW THAT KILLS ⚠️
    # The "Missing" Real-time ML Feature Lookup (The Split Brain)
    api_gw >> Edge(
        label="⛓️‍💥 NOT CONNECTED\nDelayed lookup\n(45-min behind + Online/Offline Skew)",
        style="dotted",
        color="red",
        penwidth="5"
    ) >> sagemaker

    # 3. The Analytical "Cold" Path (High Latency)
    kafka >> Edge(label="Micro-batch", color="blue") >> s3
    api_gw >> Edge(label="API Logs", style="dashed") >> s3

    # The ETL Slog (Spaghetti Tax)
    s3 >> Edge(label="Batch ETL\n(Glue Jobs)") >> glue >> Edge(label="⏱️ Weekly") >> sagemaker
    s3 >> Edge(label="Nightly Copy\n⏱️ 24hr old") >> snowflake

    # The "Dead End" (Mongo doesn't feed Snowflake - missing data)
    mongo >> Edge(label="⛓️‍💥 NOT CONNECTED\nNo schema = no modeling", style="dotted", color="red", penwidth="5") >> snowflake

    # 4. The Legacy Feedback Loop (Stale Data Problem)
    # Snowflake aggregates Postgres but misses MongoDB
    postgres >> Edge(label="Nightly Sync\n⏱️ 24hr old") >> snowflake
    snowflake >> Edge(label="Aggregates") >> airflow >> Edge(label="Daily CSV\n⏱️ 24h old", color="orange") >> falcon

    # SageMaker feeds Falcon (Static Rules - Weekly Update Cycle)
    sagemaker >> Edge(label="⏱️ Weekly Rules Push", color="darkorange") >> falcon