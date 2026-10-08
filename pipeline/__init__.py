"""The data pipeline, one subpackage per lakehouse layer:

    bronze/   sources -> bronze: CBS API (cbs), KVK file via the agent (kvk_agent),
              SBI crosswalk (reference)
    silver/   bronze -> silver: CBS tables (cbs), KVK companies (kvk)
    gold/     silver -> gold: the tables the dashboard reads (build)
    common/   shared by all layers: settings (config), publishing (publish), run log (runlog)

Run a step from the project folder, e.g. `python -m pipeline.bronze.cbs`.
"""
