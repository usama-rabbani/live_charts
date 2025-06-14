# import streamlit as st
# import plotly.graph_objects as go
# import time
# from datetime import datetime
# # import pytz  # Add pytz for timezone handling
# from db import fetch_new_execution_rows
# from producer import send_execution_to_kafka
# from consumer import get_latest_executions
# import logging

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Set Streamlit page configuration
# st.set_page_config(page_title="Live Execution Dashboard", layout="wide")
# st.title("🧪 Live Execution Results Dashboard")

# # Define Var_Desc to Var_Id mapping
# VAR_OPTIONS = [
#     {"Var_Desc": "HugoBeck-2 -> Net count", "Var_Id": 11634},
#     {"Var_Desc": "Engraving-1 -> Net Production", "Var_Id": 175},
#     {"Var_Desc": "Engraving-2 -> Net Production", "Var_Id": 1257},
#     {"Var_Desc": "Krones6 -> Production Count - IoT", "Var_Id": 5321},
#     {"Var_Desc": "Pope-Reel -> Jumbo Production", "Var_Id": 5886},
#     {"Var_Desc": "Comexi-Poc -> Net Count", "Var_Id": 5952},
#     {"Var_Desc": "proslit=PoC -> Net Count", "Var_Id": 5956},
#     {"Var_Desc": "Nexus-PoC -> Net Count", "Var_Id": 5955},
#     {"Var_Desc": "Rotomec-PoC -> Net Count", "Var_Id": 5957},
#     {"Var_Desc": "Jkamp-PoC -> Net Count", "Var_Id": 5954},
# ]

# # Create dropdown for Var_Desc (default to first option)
# var_desc_list = [option["Var_Desc"] for option in VAR_OPTIONS]
# selected_var_desc = st.selectbox("Select Variable Description", var_desc_list, index=0)

# # Get the corresponding Var_Id for the selected Var_Desc
# selected_var_id = next(option["Var_Id"] for option in VAR_OPTIONS if option["Var_Desc"] == selected_var_desc)

# # # Add a timezone selector
# # timezone_list = pytz.all_timezones
# # selected_timezone = st.selectbox("Select your timezone", timezone_list, index=timezone_list.index("Asia/Karachi"))

# # Constants
# POLL_INTERVAL = 2  # seconds
# MAX_PLOT_POINTS = 100
# KAFKA_TOPIC = "FastTopic"

# # Initialize session state
# def initialize_session_state():
#     if 'last_timestamp' not in st.session_state:
#         st.session_state.last_timestamp = datetime.min
#     if 'plot_data' not in st.session_state:
#         st.session_state.plot_data = []
#     if 'last_update' not in st.session_state:
#         st.session_state.last_update = datetime.now()
#     if 'previous_var_id' not in st.session_state:
#         st.session_state.previous_var_id = None
#     if 'is_first_run' not in st.session_state:
#         st.session_state.is_first_run = True
#     if 'total_points' not in st.session_state:
#         st.session_state.total_points = 0

# initialize_session_state()

# # Create columns for layout
# col1, col2 = st.columns([3, 1])

# # Placeholder for metrics
# with col2:
#     status_placeholder = st.empty()
#     metrics_placeholder = st.empty()

# # Placeholder for the Plotly graph
# with col1:
#     chart_placeholder = st.empty()

# def fetch_and_update_data(show_loader):
#     new_rows = fetch_new_execution_rows(st.session_state.last_timestamp, selected_var_id)
#     # logger.info(f"Fetched {len(new_rows)} new rows from database")

#     new_data_count = 0
#     if new_rows:
#         try:
#             send_execution_to_kafka(KAFKA_TOPIC, new_rows)
#             st.session_state.last_timestamp = max(
#                 datetime.fromisoformat(row['Result_On'].replace('Z', '+00:00'))
#                 if isinstance(row['Result_On'], str)
#                 else row['Result_On']
#                 for row in new_rows
#             )
#             # logger.info(f"Updated last_timestamp to {st.session_state.last_timestamp}")
#         except Exception as e:
#             # logger.error(f"Kafka send error: {e}")
#             status_placeholder.error(f"Failed to send to Kafka: {str(e)}. Continuing without Kafka update.")
#         st.session_state.plot_data.extend(new_rows)
#         new_data_count += len(new_rows)

#     messages = get_latest_executions(KAFKA_TOPIC, max_messages=10, last_timestamp=st.session_state.last_timestamp)
#     if messages:
#         st.session_state.plot_data.extend(messages)
#         st.session_state.plot_data = st.session_state.plot_data[-MAX_PLOT_POINTS:]
#         new_data_count += len(messages)
#         # logger.info(f"Received {len(messages)} new messages from Kafka")

#     if new_data_count > 0:
#         st.session_state.total_points += new_data_count

# def prepare_plot_data():
#     x, y = [], []
#     for row in st.session_state.plot_data:
#         result_on = row.get('Result_On') or row.get('ResultOn')
#         result = row.get('Result')
#         if result_on and result is not None:
#             try:
#                 if isinstance(result_on, str):
#                     result_on = datetime.fromisoformat(result_on.replace('Z', '+00:00'))
#                 result = float(result)
#                 x.append(result_on)
#                 y.append(result)
#             except (ValueError, TypeError) as e:
#                 # logger.warning(f"Invalid data format: {e}")
#                 continue
#     return x, y

# def update_metrics():
#     with metrics_placeholder.container():
#         st.metric("Total Points", st.session_state.total_points)
#         total_result = sum(
#             float(row.get('Result', 0))
#             for row in st.session_state.plot_data
#             if row.get('Result') is not None
#         )
#         count = sum(
#             1 for row in st.session_state.plot_data
#             if row.get('Result') is not None
#         )
#         average_result = total_result / count if count > 0 else 0
#         st.metric("Sum Result", total_result)
#         st.metric("Average Result", average_result)
#         # Convert server time to the user's selected timezone
#         local_time = datetime.now()
#         last_update_time = local_time.strftime("%Y-%m-%d %H:%M:%S")
#         st.metric("Last Update", last_update_time)

# def plot_chart(x, y):
#     if x and y:
#         fig = go.Figure()
#         fig.add_trace(go.Scatter(
#             x=x,
#             y=y,
#             mode='lines+markers',
#             name='Execution Result',
#             line=dict(color='royalblue', width=2),
#             marker=dict(size=6, symbol='circle')
#         ))
#         fig.update_layout(
#             title="Real-Time Execution Results",
#             xaxis_title="Timestamp",
#             yaxis_title="Result",
#             xaxis=dict(
#                 tickangle=45,
#                 tickformat="%H:%M:%S",
#                 rangeslider=dict(visible=True),
#                 type="date"
#             ),
#             yaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
#             showlegend=True,
#             template="plotly_dark",
#             height=600
#         )
#         with chart_placeholder.container():
#             st.plotly_chart(fig, use_container_width=True, key=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}")
#         status_placeholder.success("Chart updated successfully")
#     else:
#         status_placeholder.warning("No valid data to display")

# def update_dashboard():
#     var_id_changed = st.session_state.previous_var_id != selected_var_id

#     if var_id_changed:
#         st.session_state.plot_data = []  # Reset plot data
#         st.session_state.last_timestamp = datetime.min
#         st.session_state.previous_var_id = selected_var_id
#         st.session_state.total_points = 0  # Reset total points
#         # logger.info(f"Var_Id changed to {selected_var_id}, resetting plot data, timestamp, and total points")

#     show_loader = st.session_state.is_first_run or var_id_changed

#     try:
#         if show_loader:
#             with st.spinner("Loading data..."):
#                 fetch_and_update_data(show_loader)
#         else:
#             fetch_and_update_data(show_loader)

#         x, y = prepare_plot_data()
#         update_metrics()
#         plot_chart(x, y)

#         if st.session_state.is_first_run:
#             st.session_state.is_first_run = False

#     except Exception as e:
#         # logger.error(f"Dashboard update error: {e}")
#         status_placeholder.error(f"Error: {str(e)}")

# # Main loop
# while True:
#     update_dashboard()
#     time.sleep(POLL_INTERVAL)






# import streamlit as st
# import plotly.graph_objects as go
# import time
# from datetime import datetime, timezone  # timezone ko import kiya
# from db import fetch_new_execution_rows, fetch_var_options  
# from producer import send_execution_to_kafka
# from consumer import get_latest_executions
# import logging

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# # Set Streamlit page configuration
# st.set_page_config(page_title="Live Execution Dashboard", layout="wide")
# st.title(":test_tube: Live Execution Results Dashboard")

# # Fetch VAR_OPTIONS from db 
# VAR_OPTIONS = fetch_var_options()

# # if no data error generate
# if not VAR_OPTIONS:
#     st.error("No variable options found in the database. Please check the query or database connection.")
#     st.stop()

# # Create dropdown for Var_Desc (default to first option)
# var_desc_list = [option["Var_Desc"] for option in VAR_OPTIONS]
# selected_var_desc = st.selectbox("Select Variable Description", var_desc_list, index=0)

# # Get the corresponding Var_Id for the selected Var_Desc
# selected_var_id = next(option["Var_Id"] for option in VAR_OPTIONS if option["Var_Desc"] == selected_var_desc)

# # Constants
# POLL_INTERVAL = 2  # seconds
# MAX_PLOT_POINTS = 100
# KAFKA_TOPIC = "FastTopic"

# # Initialize session state
# def initialize_session_state():
#     if 'last_timestamp' not in st.session_state:
#         st.session_state.last_timestamp = datetime.min
#     if 'plot_data' not in st.session_state:
#         st.session_state.plot_data = []
#     if 'last_update' not in st.session_state:
#         st.session_state.last_update = datetime.now()
#     if 'previous_var_id' not in st.session_state:
#         st.session_state.previous_var_id = None
#     if 'is_first_run' not in st.session_state:
#         st.session_state.is_first_run = True
#     if 'total_points' not in st.session_state:
#         st.session_state.total_points = 0
#     if 'latest_result' not in st.session_state:  
#         st.session_state.latest_result = None    

# initialize_session_state()

# # Create columns for layout
# col1, col2 = st.columns([3, 1])

# # Placeholder for metrics
# with col2:
#     status_placeholder = st.empty()
#     metrics_placeholder = st.empty()

# # Placeholder for the Plotly graph
# with col1:
#     chart_placeholder = st.empty()

# def fetch_and_update_data(show_loader):
#     new_rows = fetch_new_execution_rows(st.session_state.last_timestamp, selected_var_id)
#     # logger.info(f"Fetched {len(new_rows)} new rows from database")
#     new_data_count = 0
#     if new_rows:
#         try:
#             send_execution_to_kafka(KAFKA_TOPIC, new_rows)
#             st.session_state.last_timestamp = max(
#                 datetime.fromisoformat(row['Result_On'].replace('Z', '+00:00'))
#                 if isinstance(row['Result_On'], str)
#                 else row['Result_On']
#                 for row in new_rows
#             )

#             if new_rows:
#                 latest_row = max(new_rows, key=lambda x: x['Result_On'] if isinstance(x['Result_On'], datetime) else datetime.fromisoformat(x['Result_On'].replace('Z', '+00:00')))
#                 st.session_state.latest_result = float(latest_row['Result']) if latest_row['Result'] is not None else None
#             # logger.info(f"Updated last_timestamp to {st.session_state.last_timestamp}")
#         except Exception as e:
#             # logger.error(f"Kafka send error: {e}")
#             status_placeholder.error(f"Failed to send to Kafka: {str(e)}. Continuing without Kafka update.")
#         st.session_state.plot_data.extend(new_rows)
#         new_data_count += len(new_rows)

#     messages = get_latest_executions(KAFKA_TOPIC, max_messages=10, last_timestamp=st.session_state.last_timestamp)
#     if messages:
#         st.session_state.plot_data.extend(messages)
#         st.session_state.plot_data = st.session_state.plot_data[-MAX_PLOT_POINTS:]
#         new_data_count += len(messages)
#         # logger.info(f"Received {len(messages)} new messages from Kafka")
        
#         if messages:
#             latest_message = max(messages, key=lambda x: x['Result_On'] if isinstance(x['Result_On'], datetime) else datetime.fromisoformat(x['Result_On'].replace('Z', '+00:00')))
#             st.session_state.latest_result = float(latest_message['Result']) if latest_message['Result'] is not None else None
#     if new_data_count > 0:
#         st.session_state.total_points += new_data_count

# def prepare_plot_data():
#     x, y = [], []
#     for row in st.session_state.plot_data:
#         result_on = row.get('Result_On') or row.get('ResultOn')
#         result = row.get('Result')
#         if result_on and result is not None:
#             try:
#                 if isinstance(result_on, str):
#                     result_on = datetime.fromisoformat(result_on.replace('Z', '+00:00'))
#                 result = float(result)
#                 x.append(result_on)
#                 y.append(result)
#             except (ValueError, TypeError) as e:
#                 # logger.warning(f"Invalid data format: {e}")
#                 continue
#     return x, y

# def update_metrics():
#     with metrics_placeholder.container():
#         st.metric("Total Points", st.session_state.total_points)
#         total_result = sum(
#             float(row.get('Result', 0))
#             for row in st.session_state.plot_data
#             if row.get('Result') is not None
#         )
#         count = sum(
#             1 for row in st.session_state.plot_data
#             if row.get('Result') is not None
#         )
#         average_result = total_result / count if count > 0 else 0
#         st.metric("Sum Result", f"{total_result:.3f}")
#         st.metric("Average Result", f"{average_result:.3f}")
        
#         # Last Update ko UTC time mein lete hain
#         last_update_utc = datetime.now(timezone.utc)
        
#         # HTML aur JavaScript code jo time ko local timezone mein dikhata hai
#         st.write(f"""
#         <div>Last Update: <span id='last_update'></span></div>
#         <script>
#             function updateTime() {{
#                 try {{
#                     // UTC time jo server se aaya hai
#                     var utcDate = new Date('{last_update_utc.isoformat()}');
#                     // Client ke local timezone mein convert karna
#                     var localDate = new Date(utcDate.getTime() - (utcDate.getTimezoneOffset() * 60000));
#                     // Element ko dhoondna
#                     var lastUpdateElement = document.getElementById('last_update');
#                     // Check karna ke element mil raha hai ya nahi
#                     if (lastUpdateElement) {{
#                         lastUpdateElement.innerText = localDate.toLocaleString();
#                     }} else {{
#                         console.error("Element with id 'last_update' not found");
#                     }}
#                 }} catch (e) {{
#                     console.error("Error updating time: ", e);
#                 }}
#             }}
#             // Pehle ek baar time update karo
#             updateTime();
#             // Har second baad time update karo
#             setInterval(updateTime, 1000);
#         </script>
#         """, unsafe_allow_html=True)

#         st.metric("Latest Result", st.session_state.latest_result if st.session_state.latest_result is not None else "N/A")

# def plot_chart(x, y):
#     if x and y:
#         fig = go.Figure()
#         fig.add_trace(go.Scatter(
#             x=x,
#             y=y,
#             mode='lines+markers',
#             name='Execution Result',
#             line=dict(color='royalblue', width=2),
#             marker=dict(size=6, symbol='circle')
#         ))
#         fig.update_layout(
#             title="Real-Time Execution Results",
#             xaxis_title="",
#             yaxis_title="Result",
#             xaxis=dict(
#                 tickangle=45,
#                 tickformat="",
#                 rangeslider=dict(visible=False),
#                 type="date"
#             ),
#             yaxis=dict(gridcolor='rgba(255,255,255,0.1)'),
#             showlegend=True,
#             template="plotly_dark",
#             height=600
#         )
#         with chart_placeholder.container():
#             st.plotly_chart(fig, use_container_width=True, key=f"chart_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}")
#         status_placeholder.success("Chart updated successfully")
#     else:
#         status_placeholder.warning("No valid data to display")

# def update_dashboard():
#     var_id_changed = st.session_state.previous_var_id != selected_var_id
#     if var_id_changed:
#         st.session_state.plot_data = []  # Reset plot data
#         st.session_state.last_timestamp = datetime.min
#         st.session_state.previous_var_id = selected_var_id
#         st.session_state.total_points = 0  # Reset total points
#         st.session_state.latest_result = None
#         # logger.info(f"Var_Id changed to {selected_var_id}, resetting plot data, timestamp, and total points")
#     show_loader = st.session_state.is_first_run or var_id_changed
#     try:
#         if show_loader:
#             with st.spinner("Loading data..."):
#                 fetch_and_update_data(show_loader)
#         else:
#             fetch_and_update_data(show_loader)
#         x, y = prepare_plot_data()
#         update_metrics()
#         plot_chart(x, y)
#         if st.session_state.is_first_run:
#             st.session_state.is_first_run = False
#     except Exception as e:
#         # logger.error(f"Dashboard update error: {e}")
#         status_placeholder.error(f"Error: {str(e)}")

# # Main loop
# while True:
#     update_dashboard()
#     time.sleep(POLL_INTERVAL)
