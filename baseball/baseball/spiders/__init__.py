# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.
LEAGUE_TOP = 1  # 一軍
LEAGUE_MINOR = 2  # 二軍

TEAMS = {
    'f': 'fighters',
    'h': 'hawks',
    'm': 'marines',
    'l': 'lions',
    'e': 'eagles',
    'bs': 'buffalos',
    'c': 'carp',
    'g': 'giants',
    'db': 'baystars',
    's': 'swallows',
    't': 'tigers',
    'd': 'dragons',
}


class BaseballSpidersUtil:

    @classmethod
    def get_team(cls, url):
        try:
            return TEAMS.get(url.replace('.html', '').split('_')[-1], 'Unknown')
        except IndexError:
            return 'Unknown'

    @classmethod
    def get_text(cls, text):
        """
        get text for selector
        :param text: text
        :return: str
        """
        if not text:
            return ''
        return text.replace('\u3000', ' ')

    @classmethod
    def text2digit(cls, text, digit_type=int):
        """
        get int/float for
        :param text: text
        :param digit_type: digit type(default:int)
        :return: str
        """
        if not text:
            return digit_type(0)
        try:
            return digit_type(text)
        except ValueError as e:
            return digit_type(0)