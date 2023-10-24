from airhelp.operators.ga import (
    GAExporterOperator,
    GoogleAnalyticsToSnowflakeOperator,
)


class GA360ExporterOperator(GAExporterOperator):
    @property
    def version(self) -> str:
        return "ga360"


class GA360ToSnowflakeOperator(GoogleAnalyticsToSnowflakeOperator):
    @property
    def version(self) -> str:
        return "ga360"

    @property
    def stage(self) -> str:
        return "ga_snapshots_stage"
