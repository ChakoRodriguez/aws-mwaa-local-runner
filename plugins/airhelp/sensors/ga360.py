from airhelp.sensors.ga import GATableExistenceSensor


class GA360TableExistenceSensor(GATableExistenceSensor):
    @property
    def version(self) -> str:
        return "ga360"
