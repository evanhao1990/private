# API key approach
from googleapiclient.discovery import build

def _call_translate(self, text, target, source=None):
    """
    Translate a string to the target language

    """

    service = build('translate', 'v2',
                    developerKey=os.environ.get('GOOGLE_TRANSLATE_API_KEY'))
    trans_obj = service.translations()
    result = trans_obj.list(source=source, target=target, q=[text]).execute()
    return result['translations'][0]['translatedText']


 # Client approach (preffered)
 from google.cloud import translate

 def _call_translate(self, text, target, source=None):
    """
    Translate a string to the target language

    """

    translate_client = translate.Client.from_service_account_json(
        "google-config/ppx-google-translate-creds.json"
    )

    translation = translate_client.translate(
        text, target_language=target, source_language=source)

    return translation['translatedText']