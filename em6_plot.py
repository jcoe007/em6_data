import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta

# Read the CSV file.
df = pd.read_csv("nz_energy_data.csv")

# Convert 'run_time' explicitly using inference for the ISO8601 format.
df["run_time"] = pd.to_datetime(df["run_time"], errors='raise')

# Find the most recent run_time.
most_recent_run = df["run_time"].max()
most_recent_date = most_recent_run.date()

# Define x-axis limits: from midnight of the most recent day to midnight the next day.
start_dt = pd.Timestamp(most_recent_date)
end_dt = start_dt + pd.Timedelta(days=1)

# Identify all columns ending with '_delta'
delta_cols = [col for col in df.columns if col.endswith("_delta")]

# Create a Plotly figure and add a trace for each delta column.
fig = go.Figure()
for col in delta_cols:
    fig.add_trace(go.Scatter(
        x = df["run_time"],
        y = df[col],
        mode='lines+markers',
        name = col
    ))

# Update layout to force the x-axis range.
fig.update_layout(
    title="Delta Values by Run Time",
    xaxis=dict(
        title="Run Time",
        range=[start_dt, end_dt],
        dtick=3600000,  # Tick every hour (3600000 milliseconds)
        tickformat="%H:%M"  # Format as hours and minutes.
    ),
    yaxis=dict(
        title="Delta Values"
    )
)

# Show the plot.
fig.show()
