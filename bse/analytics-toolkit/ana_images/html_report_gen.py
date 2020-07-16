import jinja2
import os
ROOT_DIR = os.path.dirname(__file__)

class CreateReport(object):

    def __init__(self):
        self.s3_url = "https://s3-eu-west-1.amazonaws.com/bse-mdd-mamba-input-prod.bseint.io/ProductLarge/"
        self.template_dir = os.path.join(ROOT_DIR, "templates", "report.html")

    def generate_report(self, styleoptions, name):
        """
        styleoptions : list
            Pass a list of styleoptions
        name : str
            Name of the .html report -> my_report for example.
        """

        template = open(self.template_dir).read()
        sids = [{"id": sid + "_001.jpg"} for sid in styleoptions]

        html_str = jinja2.Template(template).render(s3_url=self.s3_url, sids=sids)

        with open(f"{name}.html", "w") as f:
            f.write(html_str)

        return html_str