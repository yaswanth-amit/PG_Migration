"""The screen of the migration (Streamlit) and everything it needs that is not data processing.

The data pipeline lives in pipeline/ and never imports from here. Dependencies point one way: ui -> pipeline.

  components   HTML pieces of the page (cards, progress, pills)
  services     reading the tracking tables and progress file, building and launching a run
  conn_form    the Database tab: check what was typed, build the settings, test both databases
  completion   the 100% check after a run and the support package for the team
"""
