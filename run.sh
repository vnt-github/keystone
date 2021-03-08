CODE_DIR=$0
STOCKS_DATA_DIR=$PWD
HTML_FORMAT=$1
MONTH_1=$2
MONTH_2=$3
TMP_DIR=$4
cd ${CODE_DIR%run.sh}/tmp/scrapy_tutorials/scrapy_tutorials
scrapy crawl profiles_$HTML_FORMAT -a html_format=$HTML_FORMAT -a month_1=$MONTH_1 -a month_2=$MONTH_2 -a tmp_dir=$TMP_DIR -a stocks_data_dir=$STOCKS_DATA_DIR --overwrite-output=$TMP_DIR/stocks_data_$HTML_FORMAT.jsonlines --loglevel=ERROR
cd ../../../
python trade.py $HTML_FORMAT $MONTH_1 $MONTH_2 $TMP_DIR $STOCKS_DATA_DIR