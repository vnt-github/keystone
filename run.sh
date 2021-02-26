HTML_FORMAT=$1
TMP_DIR=$2
echo "HTML_FORMAT=$HTML_FORMAT"
echo "TMP_DIR=$TMP_DIR"
cd ./tmp/scrapy_tutorials/scrapy_tutorials
scrapy crawl profiles_$HTML_FORMAT -a html_format=$HTML_FORMAT
cd ../../pydoop_tutorials
hadoop fs -rm -r -f /pydoop_out/fg_industry/
hadoop fs -rm -r -f /pydoop_out/fg_$HTML_FORMAT
pydoop script industry_script.py /scrapy_out/fg_$HTML_FORMAT.jsonlines /pydoop_out/fg_industry/
pydoop script fg_score_script.py /scrapy_out/fg_$HTML_FORMAT.jsonlines /pydoop_out/fg_$HTML_FORMAT/
cd ../../
python trade_stocks.py $HTML_FORMAT > $TMP_DIR/$HTML_FORMAT-trades.txt