
from tap_mailchimp.streams.lists import ListsStream
from tap_mailchimp.streams.segments import SegmentsStream
from tap_mailchimp.streams.segment_members import SegmentsMemberStream
from tap_mailchimp.streams.list_members import ListMembersStream
from tap_mailchimp.streams.campaigns import CampaignsStream
from tap_mailchimp.streams.reports_email_activity import ReportsEmailActivityStream

AVAILABLE_STREAMS = [
    ListsStream,
    SegmentsStream,
    SegmentsMemberStream,
    ListMembersStream,
    CampaignsStream,
    ReportsEmailActivityStream
]

__all__ = [
    "ListsStream",
    "SegmentsStream",
    "SegmentsMemberStream",
    "ListMembersStream",
    "CampaignsStream",
    "ReportsEmailActivityStream"
]
