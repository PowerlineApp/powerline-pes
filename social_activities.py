
import phpserialize
import datetime

import logging
logger = logging.getLogger(__name__)

NOW         = datetime.datetime.now()
LAST_WEEK   = (NOW - datetime.timedelta(days=7)).strftime('%Y-%m-%d 00:00:00')

class SocialActivities(object):

    def __init__(self, db):
        self.db = db


    def follow_request(self, recip_id):
        """ Get the follow requesters name """
        recip_user = None
        sql = """
            SELECT firstname, lastname, username
            FROM user
            WHERE id = %s"""
        cur = self.db.cursor()
        cur.execute(sql, (recip_id,) )
        recip_user = cur.fetchone()
        cur.close()
        if recip_user:
            return recip_user

    def get_group(self, uid):
        """ """
        groups = None

        sql = """
            SELECT ug.group_id, g.official_name
            FROM users_groups ug
            INNER JOIN groups g on g.id = ug.group_id
            WHERE ug.user_id = %s """

        cur = self.db.cursor()
        cur.execute(sql, (uid,))
        groups = cur.fetchall()
        cur.close()
        return groups

    def get_saf_by_id( self, uid ):
        """ Get the last weeks SAF for a user """

        saf = None

        sql = """
            SELECT group_id, recipient_id, type, target
            FROM social_activities
            WHERE following_id = %s
            AND is_ignore IS NULL
            AND created_at > %s """

        cur = self.db.cursor()
        cur.execute(sql, (uid, LAST_WEEK) )
        saf = cur.fetchall()
        cur.close()
        return saf

    def get_saf(self, uid):
        """ Primary entry for SAF by user """

        data = {}
        saf = self.get_saf_by_id(uid)
        if saf:
            group = self.get_group( uid )
            for sa in saf:

                gid      = sa['group_id']
                recip_id = sa['recipient_id']
                sa_type  = sa['type']

                try:
                    target  = phpserialize.loads( sa['target'] )
                except:
                    logger.error("Issue unserializing SAF target data")
                    continue

                if sa_type == 'follow-request':
                    # basic follow reqest

                    if sa_type not in data:
                       data[sa_type] = []

                    fr = self.follow_request(recip_id)
                    data[sa_type].append( fr )

                elif sa_type == 'follow-micropetitionCommented':
                    pass

                elif sa_type == 'micropetition-created':
                    pass

                elif sa_type == 'comment-mentioned':
                    pass

                elif sa_type == 'comment-replied':
                    pass

                elif sa_type == 'answered':

                    """
                    if sa_type not in data:
                        data[sa_type] = []

                    # label + type = answer type
                    #
                    # label ::= [ petition, payment request, question, event ]
                    # type  ::= [ group_petition, group_payment_request_crowdfunding,
                    #             group_payment_request, group_question, group_event ]
                    # Example:
                    #   New Petition
                    #       label : petition
                    #       type  : group_question
                    #

                    fr = self.follow_request(recip_id)
                    data[sa_type].append( fr )
                    """
                    pass

                elif sa_type == 'follow-pollCommented':
                    pass

        return data
