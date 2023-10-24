from airhelp.sensors.ga import GATableExistenceSensor


class GA4TableExistenceSensor(GATableExistenceSensor):
    @property
    def version(self) -> str:
        return "ga4"
