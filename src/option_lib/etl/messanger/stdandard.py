from option_lib.etl.messanger.message_class import AbstractMessanger


class StandardMessanger(AbstractMessanger):
    """Stdout messaging"""

    def send_message(self, text):
        print(text)
