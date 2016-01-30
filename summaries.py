#!/usr/bin/env python

from pprint import pprint
import social_activities

import boto.ses
#import boto.ses.BotoServerError
import MySQLdb
import MySQLdb.cursors

import os
import sys
import json
import datetime
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

CONFIGFILE = "{0}/summaries.json".format(os.getcwd() )
LOGFILE    = "/srv/log/digest-summaries.log"
TPL_DIR    = "{dir:s}/templates".format(dir=os.path.dirname(os.path.abspath(__file__)))

NOW = datetime.datetime.now()

# setup logging
import logging
logger = logging.getLogger('digest-summaries')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler(LOGFILE)
fh.setFormatter(formatter)
logger.addHandler(fh)

# pes-postman

class PLEmailSummaries(object):

    def __init__(self, access, secret, region='us-east-1' ):

        self.region = region
        self.access = access
        self.secret = secret

        self.base_file   = "{dir:s}/base.html".format(dir=TPL_DIR)
        self.body_file   = "{dir:s}/body.html".format(dir=TPL_DIR)
        self.footer_file = "{dir:s}/footer.html".format(dir=TPL_DIR)

        self.email_source = "support@powerli.ne"
        self.email_subj = "Hey {fname:s}! Here's what's happening this week."

        self.ses = boto.ses.connect_to_region(
                    self.region,
                    aws_access_key_id=self.access,
                    aws_secret_access_key=self.secret )

        with open(self.base_file, 'r') as f:
            self.base_html = f.read()

        with open(self.body_file, 'r') as f:
            self.body_html = f.read()

        with open(self.footer_file, 'r') as f:
            self.footer_html = f.read()

    def build_follow_request(self, data):
        """ Build follow request content """

        num = len(data)
        if num > 5:
            limit = 5
        else:
            limit = num

        content = "<ul>"
        for idx, f in enumerate(data):
            if idx >= limit:
                break
            first = f['firstname']
            last  = f['lastname']
            user  = f['username']
            follow = "<li> {f:s} {l:s} ({u:s}) wants to follow you!</li>".format(f=first, l=last, u=user)
            content = content + follow

        content = content + "</ul>"
        return content

    def build_body(self, fname, saf):
        """ Build the body of the email """

        body_tpl = Template(self.body_html)
        for type, sa in saf.iteritems():
            if type =='follow-request':
                follows = self.build_follow_request(sa)
        return body_tpl.substitute(firstname=fname, content=follows)

    def send(self, fname, addr, html ):
        """ Send the email via SES """

        message = MIMEMultipart()

        message['Subject']  = self.email_subj.format(fname=fname)
        message['From']     = self.email_source
        message['To']       = addr
        message.attach( MIMEText(html, 'html') )

        self.ses.send_raw_email(
            message.as_string(),
            source=message['From'],
            destinations=message['To'] )

    def send_summaries(self, email_data):
        """ Send the email summaries """

        base_tpl = Template(self.base_html)
        for uid, data in email_data.iteritems():

            if data['data']:

                addr  = data['meta']['email']
                fname = data['meta']['fname']
                saf   = data['data']

                body_html = self.build_body(fname, saf)
                email_html = base_tpl.substitute(body=body_html, footer=self.footer_html)

                try:
                    self.send(fname, addr, email_html)
                except Exception as e:
                    logger.error("Issue sending email to {email}: {err}".format(email=addr, err=e))
                    continue

class PLDataAccess(object):

    def __init__(self, host, user, passwd, dbname):

        self.host = host
        self.user = user
        self.passwd = passwd
        self.dbname = dbname

        self.db = MySQLdb.connect(
                host=self.host,
                user=self.user,
                passwd=self.passwd,
                db=self.dbname,
                cursorclass=MySQLdb.cursors.DictCursor)

    def get_user_data(self):
        """ Return a list of emails and users who want to be
            informed of their weekly activities """

        # for now, we are just fetching all emails as this will run
        # every sunday night

        # TODO: I also need to add the weekly / daily / off check
        #       to the sql as i'm just getting all emails

        sql = """
            SELECT id, firstname, email
            FROM user """

        cur = self.db.cursor()
        cur.execute(sql)
        users = cur.fetchall()
        cur.close()
        return users

    def get_email_content(self):
        """ Get content """

        content = {}

        # Primary means to get user emails we want to work with
        users_data = self.get_user_data()

        # Initialize the SA obj so that we can get the SAF for
        # each user
        sa = social_activities.SocialActivities(self.db)

        for user in users_data:

            uid      = int(user['id'])
            fname    = user['firstname']
            email    = user['email']

            content[uid] = {}
            content[uid]['meta'] = { 'fname' : fname, 'email' : email }

            # Build the social activities emailable data
            saf = sa.get_saf( uid )
            content[uid]['data'] = saf

        return content


def load_config():
    """ Load the config file """
    with open(CONFIGFILE, 'r') as f:
        c = json.load( f )
    return ( c['aws'], c['mysql'] )

def main():

    (aws_cfg, mysql_cfg) = load_config()

    plda = PLDataAccess(
            mysql_cfg['host'],
            mysql_cfg['user'],
            mysql_cfg['passwd'],
            mysql_cfg['dbname'])
    email_data = plda.get_email_content()

    emailclient = PLEmailSummaries(
            aws_cfg['access'],
            aws_cfg['secret'],
            aws_cfg['region'])

    emailclient.send_summaries(email_data)

if __name__ == "__main__":

    try:
        main()
    #except boto.ses.BotoServerError as err:
    #    logger.error("Issue connecting to AWS")
    #    sys.exit(1)
    except Exception as e:
        import traceback
        print e, traceback.format_exc()
        sys.exit(1)
