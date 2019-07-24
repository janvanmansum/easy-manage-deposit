#!/usr/bin/env bash
#
# Helper script to create full and summary reports and send them to a list of recipients.
#
# Use - (dash) as depositor-account to generate a report for all the deposits.
#

usage() {
    echo "Usage: create-and-send-report [-s, --send-always] <host-name> <depositor-account> [<from-email>] <to-email> [<bcc-email>]"
    echo "       create-and-send-report --help"
}

SEND_ALWAYS=false

while true; do
    case "$1" in
        -h | --help) usage; exit 0 ;;
        -s | --send-always)
            SEND_ALWAYS=true
            shift 1
        ;;
        *) break;;
    esac
done

EASY_HOST=$1
EASY_ACCOUNT=$2
FROM=$3
TO=$4
BCC=$5
TMPDIR=/tmp

if [[ "$EASY_ACCOUNT" == "-" ]]; then
    EASY_ACCOUNT=""
fi

DATE=$(date +%Y-%m-%d)
REPORT_SUMMARY=${TMPDIR}/report-summary-${EASY_ACCOUNT:-all}-$DATE.txt
REPORT_SUMMARY_24=${TMPDIR}/report-summary-${EASY_ACCOUNT:-all}-yesterday-$DATE.txt
REPORT_FULL=${TMPDIR}/report-full-${EASY_ACCOUNT:-all}-$DATE.csv
REPORT_FULL_24=${TMPDIR}/report-full-${EASY_ACCOUNT:-all}-yesterday-$DATE.csv

if [[ "$FROM" == "" ]]; then
    FROM_EMAIl=""
else
    FROM_EMAIL="-r $FROM"
fi

if [[ "$BCC" == "" ]]; then
    BCC_EMAILS=""
else
    BCC_EMAILS="-b $BCC"
fi

TO_EMAILS="$TO"

exit_if_failed() {
    local EXITSTATUS=$?
    if [[ $EXITSTATUS != 0 ]]; then
        echo "ERROR: $1, exit status = $EXITSTATUS"
        echo "Report generation FAILED. Contact the system administrator." |
        mail -s "FAILED: $EASY_HOST Report: status of EASY deposits (${EASY_ACCOUNT:-all depositors})" \
             $FROM_EMAIL $BCC_EMAILS $TO
        exit 1
    fi
    echo "OK"
}

echo -n "Creating summary report for ${EASY_ACCOUNT:-all depositors}..."
/opt/dans.knaw.nl/easy-manage-deposit/bin/easy-manage-deposit report summary $EASY_ACCOUNT > $REPORT_SUMMARY
exit_if_failed "summary report failed"

echo -n "Creating summary report from the last 24 hours for ${EASY_ACCOUNT:-all depositors}..."
/opt/dans.knaw.nl/easy-manage-deposit/bin/easy-manage-deposit report summary --age 0 $EASY_ACCOUNT > $REPORT_SUMMARY_24
exit_if_failed "summary report failed"

echo -n "Creating full report for ${EASY_ACCOUNT:-all depositors}..."
/opt/dans.knaw.nl/easy-manage-deposit/bin/easy-manage-deposit report full $EASY_ACCOUNT > $REPORT_FULL
exit_if_failed "full report failed"

echo -n "Creating full report from the last 24 hours for ${EASY_ACCOUNT:-all depositors}..."
/opt/dans.knaw.nl/easy-manage-deposit/bin/easy-manage-deposit report full --age 0 $EASY_ACCOUNT > $REPORT_FULL_24
exit_if_failed "full report failed"

echo "Counting the number of lines in $REPORT_FULL_24; if there is only a header (a.k.a. 1 line), no deposits were found and sending a report is not needed..."
LINE_COUNT=$(wc -l < "$REPORT_FULL_24")
echo "Line count in $REPORT_FULL_24: $LINE_COUNT line(s)."

if [[ $LINE_COUNT -gt 1 || "$SEND_ALWAYS" = true ]]; then
    echo "New deposits detected, therefore sending the report"

    echo "Status of $EASY_HOST deposits d.d. $(date) for depositor: ${EASY_ACCOUNT:-all}" | \
    mail -s "$EASY_HOST Report: status of EASY deposits (${EASY_ACCOUNT:-all depositors})" \
         -a $REPORT_SUMMARY \
         -a $REPORT_SUMMARY_24 \
         -a $REPORT_FULL \
         -a $REPORT_FULL_24 \
         $BCC_EMAILS $FROM_EMAIL $TO
    exit_if_failed "sending of e-mail failed"
else
    echo "No new deposits were done, therefore no report was sent."
fi

echo -n "Remove generated report files..."
rm -f $REPORT_SUMMARY && \
rm -f $REPORT_SUMMARY_24 && \
rm -f $REPORT_FULL && \
rm -f $REPORT_FULL_24
exit_if_failed "removing generated report file failed"
