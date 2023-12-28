import os


class State:
    @classmethod
    def get_state(cls):
        if os.path.exists(settings.SEARCH_STATE_FILEPATH):
            state = cls._read_state()
            formatted_state = {
                key: datetime.strptime(value, settings.SEARCH_STATE_TIME_FORMAT)
                if value
                else None
                for key, value in state.items()
            }
            return formatted_state
        cls.set_default()

    @classmethod
    def set_state(cls, **kwargs):
        current_state = cls._read_state()
        with open(settings.SEARCH_STATE_FILEPATH, "w") as f:
            updated_states = {
                key: value.strftime(settings.SEARCH_STATE_TIME_FORMAT)
                for key, value in kwargs.items()
                if value is not None
            }
            current_state.update(**updated_states)
            json.dump(current_state, f)

    @staticmethod
    def set_default():
        with open(settings.SEARCH_STATE_FILEPATH, "w") as f:
            template = {field.default: None for field in fields(ModelNames)}
            json.dump(template, f)

    @staticmethod
    def _read_state():
        with open(settings.SEARCH_STATE_FILEPATH, "r") as f:
            return json.load(f)
