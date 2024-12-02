# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Write directly to the app
st.title(f"Streamlit App Placeholder ({st.__version__})")
st.write(
  """Replace this with your own code!
  **And if you're new to Streamlit,** check
  out the easy-to-follow guides at
  [docs.streamlit.io](https://docs.streamlit.io).
  """
)

# Get the current credentials
session = get_active_session()

