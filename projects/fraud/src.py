import smtplib


def sent_email(recipients, subject, body):
    user = 'hzhang@harlemnext.com'
    password = 's1oCrit_Ug'

    email_text = f'From: {user}\nTo: {",".join(recipients)}\nSubject: {subject}\n\n{body}\n'

    try:
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        server.ehlo()
        server.login(user, password)
        server.sendmail(user, recipients, email_text)
        server.close()
        print('Email sent!')
    except:
        print('Something went wrong...')

# client = bigquery.Client(project = "alg-hn-insights")
