from airhelp.operators.ga import (
    GAExporterOperator,
    GoogleAnalyticsToSnowflakeOperator,
)


class GA4ExporterOperator(GAExporterOperator):
    @property
    def version(self) -> str:
        return "ga4"


class GA4ToSnowflakeOperator(GoogleAnalyticsToSnowflakeOperator):
    @property
    def version(self) -> str:
        return "ga4"

    @property
    def stage(self) -> str:
        return f"ga4_snapshots_dt_{self.environment}_stage"
