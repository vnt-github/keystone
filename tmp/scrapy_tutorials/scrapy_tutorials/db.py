import pydoop.hdfs as hdfs
from itemadapter import ItemAdapter
from json import dumps

class PydoopPipeline(object):
    def open_spider(self, spider):
        self.output_dir = spider.tmp_dir
        self.output_file = f"fg_{spider.html_format}.jsonlines"
        hdfs.mkdir(f"{self.output_dir}")
        self.f = hdfs.open(f"{self.output_dir}/{self.output_file}", "wt")

    def close_spider(self, spider):
        self.f.close()

    def process_item(self, item, spider):
        data = ItemAdapter(item).asdict()
        self.f.write(dumps(data)+"\n")
        return item