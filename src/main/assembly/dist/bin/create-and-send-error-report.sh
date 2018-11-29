#!/usr/bin/env bash
#
# Helper script to create error reports and send them to a list of recipients.
#
# Usage: ./create-and-send-error-report.sh <host-name> <depositor-account> [<from-email>] <to-email> [<bcc-email>]
#
# Use - (dash) as depositor-account to generate a report for all the deposits.
#

EASY_HOST=$1
EASY_ACCOUNT=$2
FROM=$3
TO=$4
BCC=$5
TMPDIR=/tmp

if [ "$EASY_ACCOUNT" == "-" ]; then
    EASY_ACCOUNT=""
fi

DATE=$(date +%Y-%m-%d)
REPORT_ERROR=${TMPDIR}/report-error-${EASY_ACCOUNT:-all}-$DATE.csv
REPORT_ERROR_24=${TMPDIR}/report-error-${EASY_ACCOUNT:-all}-yesterday-$DATE.csv


if [ "$FROM" == "" ]; then
    FROM_EMAIl=""
else
    FROM_EMAIL="-r $FROM"
fi

if [ "$BCC" == "" ]; then
    BCC_EMAILS=""
else
    BCC_EMAILS="-b $BCC"
fi

TO_EMAILS="$TO"

exit_if_failed() {
    local EXITSTATUS=$?
    if [ $EXITSTATUS != 0 ]; then
        echo "ERROR: $1, exit status = $EXITSTATUS"
        echo "Error report generation FAILED. Contact the system administrator." |
        mail -s "FAILED: Error report: status of failed EASY deposits (${EASY_ACCOUNT:-all depositors})" \
             $FROM_EMAIL $BCC_EMAILS $TO
        exit 1
    fi
    echo "OK"
}

echo -n "Creating error report for ${EASY_ACCOUNT:-all depositors}..."
/opt/dans.knaw.nl/easy-manage-deposit/bin/easy-manage-deposit report error $EASY_ACCOUNT > $REPORT_ERROR
exit_if_failed "error report failed"

echo -n "Creating error report from the last 24 hours for ${EASY_ACCOUNT:-all depositors}..."
/opt/dans.knaw.nl/easy-manage-deposit/bin/easy-manage-deposit report error --age 0 $EASY_ACCOUNT > $REPORT_ERROR_24
exit_if_failed "error report failed"

echo "Counting the number of lines in $REPORT_ERROR_24; if there is only a header (a.k.a. 1 line), no failed deposits were found and sending a report is not needed..."
LINE_COUNT=$(wc -l < "$REPORT_ERROR_24")
echo "Line count in $REPORT_ERROR_24: $LINE_COUNT line(s)."

if [ $LINE_COUNT -gt 1 ]; then
    echo "New failed deposits detected, therefore sending the report"

    echo "Status of $EASY_HOST deposits d.d. $(date) for depositor: ${EASY_ACCOUNT:-all}" | \
    mail -s "$EASY_HOST Error report: status of failed EASY deposits (${EASY_ACCOUNT:-all depositors})" \
         -a $REPORT_ERROR \
         -a $REPORT_ERROR_24 \
         $BCC_EMAILS $FROM_EMAIL $TO
    exit_if_failed "sending of e-mail failed"
else
    echo "No new failed deposits were found, therefore no report was sent."
fi
