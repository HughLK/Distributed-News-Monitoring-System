# -*- coding: utf-8 -*-

from lxml import etree
import re
from collections import OrderedDict
    
def news_site_get_time(body):
    body = body.encode('utf-8')
    # 去掉JS中时间的影响
    pattern = r'''<SCRIPT.*?>.*?</SCRIPT>|<script.*?>.*?</script>'''
    sub_body = re.sub(pattern, "", body, flags=re.S|re.M)

    # YYYY-MM-DD hh:mm \ YYYY年MM月DD日 hh:mm \ YYYY年MM月DD日hh:mm
    time_pattern = re.compile(r'''(2\d{3}-\d{2}-\d{2}\s*\d{2}:\d{2})|
            (2\d{3}.{3\d{2}.{3}\d{2}.*?\s*\d{2}:\d{2})''')
    # YYYY-MM-DD \ YYYY年MM月DD日
    time_pattern2 = re.compile(r'''(2\d{3}-\d{1,2}-\d{1,2})|
            (2\d{3}.{3}\d{1,2}.{3}\d{1,2})''')
    time = re.findall(time_pattern, sub_body)
    time2 = re.findall(time_pattern2, sub_body)

    if len(time):
        time = time[0][0]
        
        # 统一格式
        # '年月'匹配
        year_mon = re.compile(u'''(\\xe5\\xb9\\xb4)|(\\xe6\\x9c\\x88)''')
        time = re.sub(year_mon, "-", time)
        # '日'匹配
        day = re.compile(u'''\\xe6\\x97\\xa5\\s*''')
        time = re.sub(day, " ", time)
        return time
    elif len(time2):
        time = time2[0][0]
        
        # 统一格式
        # '年月'匹配
        year_mon = re.compile(u'''(\\xe5\\xb9\\xb4)|(\\xe6\\x9c\\x88)''')
        time = re.sub(year_mon, "-", time)
        # '日'匹配
        day = re.compile(u'''\\xe6\\x97\\xa5\\s*''')
        time = re.sub(day, " ", time)
        return time
    # 导航页匹配的时间数很大
    else:
        return None

def site_get_content(body, threshold = 4000):
    # 预处理 去掉换行和圆角空格
    replace_blank = r'''\u3000|\xa0|\n'''
#    body = re.sub(replace_blank, "", body, flags=re.S|re.M)
    body = body.replace(replace_blank, "")
    
    # 预处理 去掉标签内纯空格内容
    replace_blank = r'''>\s+<'''
#    body = re.sub(replace_blank, "><", body, flags=re.S|re.M)
    body = body.replace(replace_blank, "><")
    
    # 预处理 去掉无意义标签
    pattern = r'''<SCRIPT.*?>.*?</SCRIPT>|<script.*?>.*?</script>|<strong>|</strong>|<b>|</b>|<pre>|</pre>|<style.*?>.*?</style>|<del>|</del>|<ins.*?>|</ins>|<noscript>.*?</noscript>|<img.*?>|<!--.*?-->|<a.*?>.*?</a>|<br\s*/>|<div id="footer">.*?</div>|<textarea.*?>.*?</textarea>'''
    sub_body = re.sub(pattern, "", body, flags=re.S|re.M)
    
    # 建立DOM树
    root = etree.HTML(sub_body)
    tree = etree.ElementTree(root)

    style_paths = OrderedDict()
    for e in root.iter():

#        if not len(e.getchildren()) and e.text and e.text != "":
#            parent_path_of_leaf = tree.getpath(e.getparent())
#            style_paths[parent_path_of_leaf] = style_paths.get(parent_path_of_leaf, [])
#            style_paths[parent_path_of_leaf].append(e.text.strip())
        # 文本在标签下
        text = ""
        if e.text and e.text != "":
            text = e.text
        # 文本被标签分割或未被标签包围
        elif e.tail and e.tail != "":
            text = e.tail
        
        if text != "":
            path = tree.getpath(e)
            # 修改路径使同一类型叶节点为一种路径
            path_pattern = re.compile(r'''^(.*?)\[\d\]$''')
            path_re = re.findall(path_pattern, tree.getpath(e))
            if len(path_re):
                path = path_re[0]
    
            style_paths[path] = style_paths.get(path, [])
            style_paths[path].append(text.strip())
    
    # 计算文本、符号密度、综合密度
    text_density = OrderedDict()
    symbol_density = OrderedDict()
    syn_density = OrderedDict()
    # 提取中文符号的正则模式
    symbol_extract = r'''[\w\d\s\.\\]'''
    
    for key, value in style_paths.items():
        values_join = "".join(value)
        length = len(value)
        
        # 文本密度
        text_density[key] = len(values_join) / length
        
        # 符号密度
#        off_symbol_value = re.sub(symbol_extract, "", values_join, flags=re.S|re.M)
        off_symbol_value = values_join.replace(symbol_extract, "")
        # 加0.1防止文字无标点其密度为0
        symbol_density[key] = len(off_symbol_value) / length + 0.01
        
        # 综合密度
        syn_density[key] = text_density[key] * symbol_density[key]
    
    # 综合密度大于阈值的文本
    extracted_texts = ["".join(style_paths[key]) for key, value in syn_density.items() if value > threshold]
#    print (style_paths)
#    print ('\n')
#    print (syn_density)
    return "".join(extracted_texts)
