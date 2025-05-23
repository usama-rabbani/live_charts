import streamlit as st
import plotly.graph_objects as go
import time
from datetime import datetime
from db import fetch_new_execution_rows
from producer import send_execution_to_kafka
from consumer import get_latest_executions
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Set Streamlit page configuration
st.set_page_config(page_title="Live Execution Dashboard", layout="wide")
st.title("🧪 Live Execution Results Dashboard")

# Define Var_Desc to Var_Id mapping
VAR_OPTIONS = [
    {"Var_Desc": "HugoBeck-2 -> Net count", "Var_Id": 11634},
    {"Var_Desc": "Beiren 2-PoC -> NetCount", "Var_Id": 5924},
    {"Var_Desc": "Keifel 1 -PoC -> Net Count", "Var_Id": 5950},
    {"Var_Desc": "Beiren 1-PoC -> Net Count", "Var_Id": 5949},
    {"Var_Desc": "Pope-Reel -> Jumbo Production", "Var_Id": 5886},
    {"Var_Desc": "Comexi-Poc -> Net Count", "Var_Id": 5952},
    {"Var_Desc": "proslit=PoC -> Net Count", "Var_Id": 5956},
    {"Var_Desc": "Nexus-PoC -> Net Count", "Var_Id": 5955},
    {"Var_Desc": "Rotomec-PoC -> Net Count", "Var_Id": 5957},
    {"Var_Desc": "Jkamp-PoC -> Net Count", "Var_Id": 5954},
]

# Create dropdown for Var_Desc (default to first option)
var_desc_list = [option["Var_Desc"] for option in VAR_OPTIONS]
selected_var_desc = st.selectbox("Select Variable Description", var_desc_list, index=0)

# Get the corresponding Var_Id for the selected Var_Desc
selected_var_id = next(option["Var_Id"] for option in VAR_OPTIONS if option["Var_Desc"] == selected_var_desc)

# Constants
POLL_INTERVAL = 2  # seconds
MAX_PLOT_POINTS = 100
KAFKA_TOPIC = "FastTopic"

# Initialize session state
def initialize_session_state():
    if 'last_timestamp' not in st.session_state:
        st.session_state.last_timestamp = datetime.min
    if 'plot_data' not in st.session_state:
        st.session_state.plot_data = []
    if 'last_update' not in st.session_state:
        st.session_state.last_update = datetime.now()
    if 'previous_var_id' not in st.session_state:
        st.session_state.previous_var_id = None
    if 'is_first_run' not in st.session_state:
        st.session_state.is_first_run = True
    if 'total_points' not in st.session_state:
        st.session_state.total_points = 0  

initialize_session_state()

# Create columns for layout
col1, col2 = st.columns([3, 1])

# Placeholder for metrics
with col2:
    status_placeholder = st.empty()
    metrics_placeholder = st.empty()

# Placeholder for the Plotly graph
with col1:
    chart_placeholder = st.empty()

def fetch_and_update_data(show_loader):
    # Fetch new rows from database
    new_rows = fetch_new_execution_rows(st.session_state.last_timestamp, selected_var_id)
    logger.info(f"Fetched {len(new_rows)} new rows from database")

    new_data_count = 0
    if new_rows:
        try:
            send_execution_to_kafka(KAFKA_TOPIC, new_rows)
            st.session_state.last_timestamp = max(
                datetime.fromisoformat(row['Result_On'].replace('Z', '+00:00'))
                if isinstance(row['Result_On'], str)
                else row['Result_On']
                for row in new_rows
            )
            logger.info(f"Updated last_timestamp to {st.session_state.last_timestamp}")
        except Exception as e:
            logger.error(f"Kafka send error: {e}")
            status_placeholder.error(f"Failed to send to Kafka: {str(e)}. Continuing without Kafka update.")
        st.session_state.plot_data.extend(new_rows)
        new_data_count += len(new_rows)

    # Fetch new messages from Kafka using last_timestamp
    messages = get_latest_executions(KAFKA_TOPIC, max_messages=10, last_timestamp=st.session_state.last_timestamp)
    if messages:
        st.session_state.plot_data.extend(messages)
        st.session_state.plot_data = st.session_state.plot_data[-MAX_PLOT_POINTS:]
        new_data_count += len(messages)  # Only count new messages
        logger.info(f"Received {len(messages)} new messages from Kafka")

    # Update total_points only for new data
    if new_data_count > 0:
        st.session_state.total_points += new_data_count

def prepare_plot_data():
    x, y = [], []
    for row in st.session_state.plot_data:
        result_on = row.get('Result_On') or row.get('ResultOn')
        result = row.get('Result')
        if result_on and result is not None:
            try:
                if isinstance(result_on, str):
                    result_on = datetime.fromisoformat(result_on.replace('Z', '+00:00'))
                result = float(result)
                x.append(result_on)
                y.append(result)
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid data format: {e}")
                continue
    return x, y

def update_metrics():
    with metrics_placeholder.container():
        st.metric("Total Points", st.session_state.total_points)
        total_result = sum(
            float(row.get('Result', 0))
            for row in st.session_state.plot_data
            if row.get('Result') is not None
        )
        count = sum(
            1 for row in st.session_state.plot_data
            if row.get('Result') is not None
        )
        average_result = total_result / count if count > 0 else 0
        st.metric("Sum Result", total_result)
        st.metric("Average Result", average_result)
        st.metric("Last Update", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def plot_chart(x, y):
    if x and y:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=x,
            y=y,
            mode='lines+markers',
            name='Execution Result',
            line=dict(color='royalblue', width=2),
            marker=dict(size=6, symbol='circle')
        ))
        fig.update_layout(
            title="Real-Time Execution Results",
            xaxis_title="Timestamp",
            yaxis_title="Result",
            xaxis=dict(
                tickangle=45,
                tickformat="%H:%M:%S",
                rangeslider=dict(visible=True),
                type="date"
            ),
            yaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
            showlegend=True,
            template="plotly_dark",
            height=600
        )
        with chart_placeholder.container():
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}")
        status_placeholder.success("Chart updated successfully")
    else:
        status_placeholder.warning("No valid data to display")

def update_dashboard():
    var_id_changed = st.session_state.previous_var_id != selected_var_id

    if var_id_changed:
        st.session_state.plot_data = []
        st.session_state.last_timestamp = datetime.min
        st.session_state.previous_var_id = selected_var_id
        logger.info(f"Var_Id changed to {selected_var_id}, resetting plot data and timestamp")

    show_loader = st.session_state.is_first_run or var_id_changed

    try:
        if show_loader:
            with st.spinner("Loading data..."):
                fetch_and_update_data(show_loader)
        else:
            fetch_and_update_data(show_loader)

        x, y = prepare_plot_data()
        update_metrics()
        plot_chart(x, y)

        if st.session_state.is_first_run:
            st.session_state.is_first_run = False

    except Exception as e:
        logger.error(f"Dashboard update error: {e}")
        status_placeholder.error(f"Error: {str(e)}")

# Main loop
while True:
    update_dashboard()
    time.sleep(POLL_INTERVAL)