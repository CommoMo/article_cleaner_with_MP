import multiprocessing
import logging
import re
import os
import json
from time import time

from nltk.tokenize import sent_tokenize
from tqdm import tqdm

# import nltk
# nltk.download("punkt")

root_path = os.getcwd()
src_dir_name = ''
target_dir_name = ''

src_dir_path = os.path.join(root_path, src_dir_name)
target_path = os.path.join(root_path, target_dir_name)


class LM_Preprocessor(object):
    def __init__(self, MIN_SENT_LEN=10, MAX_SENT_LEN=500, MIN_KOREAN_RATE=0.4, is_wiki=False, is_news=False):
        self.korean_regex = re.compile('[¤¡-¤¾|¤¿-¤Ó|°¡-ÆR|0-9]+')
        self.patterns = [re.compile(r'\[.{1,15}\]'), 
                         re.compile(r'\(.{1,15}\)'), 
                         re.compile(r'\<.{1,15}\>'), 
                         re.compile(r'\¡¼.{1,15}\¡½'), 
                         re.compile(r'[¤¡-¤¾|¤¿-¤Ó|°¡-ÆR]*\s±âÀÚ[¤¡-¤¾|¤¿-¤Ó|°¡-ÆR]{0,5}[\.\]\)]'), 
                         re.compile(r'[¤¡-¤¾|¤¿-¤Ó|°¡-ÆR]*\sÆ¯ÆÄ¿ø[¤¡-¤¾|¤¿-¤Ó|°¡-ÆR]{0,5}[\.\]\)]'), 
                         re.compile(r'.*»çÁø\s?[=/].*'),
                         re.compile(r'\(?[¤¡-¤¾|¤¿-¤Ó|°¡-ÆR|0-9]*[ìé-ûù?-?][¤¡-¤¾|¤¿-¤Ó|°¡-ÆR|0-9]*\)?'),
                         re.compile(r'^¡ã.*'),
                         re.compile(r'.*¨Ï.*'),
                         #re.compile(r'\(.*°ü·Ã ±â»ç:.*\)'),
                         re.compile(r'(\[)?[0-9a-zA-Z]([-_.]?[0-9a-zA-Z])*@[0-9a-zA-Z]([-_.]?[0-9a-zA-Z])*.[a-zA-Z]{2,3}(\])?')]
        
        self.MIN_SENT_LEN = MIN_SENT_LEN
        self.MAX_SENT_LEN = MAX_SENT_LEN
        self.MIN_KOREAN_RATE = MIN_KOREAN_RATE
        
        self.is_wiki = is_wiki
        self.is_news = is_news
        self.except_text_list = ['ref', 'http://', 'https://', 'ISBN', '.jpg', 'icon']
        self.except_start_list = ['L', '!']
        
        self.list_noreturn = ['[[File:', '[[Image:','{|', '|}', 'class=', 'style=']        
        self.wiki_table = re.compile('!{2,2}[^!]+!{2,2}')
        self.dict_replacetoken_to_none = {"\'\'\'":"", "\'\'":"", '&amp;':'&', '</ref>':''}
        
    def run(self, text_chunk):
        # ¹®Àå ºÐ¸®
        try:
            list_sent = []
            sentences = []

            nlsplit = text_chunk.split('\n')
            nlsplit.insert(0, '')
            nlsplit.append('')

            for i in range(1, len(nlsplit) - 1):
                if nlsplit[i] != nlsplit[i + 1] and nlsplit[i] != nlsplit[i - 1]:   #»çÁø¼³¸í »èÁ¦
                    for sentence in sent_tokenize(nlsplit[i]):
                        sentence = self.process_news_text(sentence)
                        
                        if '\n' in sentence:
                            sentences.extend(sentence.split('\n'))
                        else:
                            sentences.append(sentence)
           
            for sent in sentences:
              
                #if sent[-1] != '.':
                    # logger.debug(f"[REMOVE][NOT_PERFECT_SENTENCCE] {sent}")
                    #continue
                # ¹®Àå ±æÀÌ Ã¼Å©
                if len(sent) < self.MIN_SENT_LEN:
                    # logger.debug(f"[REMOVE][MIN_SENT_LEN] {sent}")
                    continue
                if len(sent) > self.MAX_SENT_LEN:
                    # logger.debug(f"[REMOVE][MAX_SENT_LEN] {sent}")
                    continue
                # ÇÑ±¹¾î ºñÀ² Ã¼Å© 
                match_sent = self.korean_regex.findall(sent)
                if len(''.join(match_sent)) / len(sent) < self.MIN_KOREAN_RATE:
                    # logger.debug(f"[REMOVE][MIN_KOREAN_RATE] {sent}")
                    continue
                        
                if self.is_news:
                    sent = self.process_news_text(sent)
                    #logger.debug(f"[REMOVE][NEWS_FILTER] {sent}")
      
                list_sent.append(sent)

        except:
            # logger.debug(f"[ERROR] {text_chunk}")
         
            pass
        

        return list_sent
    
    def process_news_text(self, sent):
        for pattern in self.patterns:
            sent = pattern.sub('', sent)
        return sent
    
    
def load_articles(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        print('Fail to load: {}'.format(file_path))
        return False

def save_articles(save_path, articles):
    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent='\t')
        
def worker(list_line, q):
    list_result = []
    for line in list_line:
        result = preprocessor.run(line['contents'])
        if len(result) < 2:
            continue
        else:
            line['contents'] = ' '.join(result)
        list_result.append(line)
    return list_result




def process_file(file_name):
    start_time = time()
    print("  >> doing...: {}".format(file_name))

    file_path = os.path.join(src_dir_path, file_name)
    articles = load_articles(file_path)
    
    buff_size = 10
    workers = []

    for i in range(0, len(articles), buff_size):
        end_idx = len(articles) if i+buff_size > len(articles) else i+buff_size
        buff = articles[i:end_idx]
        workers.append(pool.apply_async(worker, (buff, q)))

    save_list = []
    for res in tqdm(workers):
        save_list.extend(res.get())

    print('taken: {:.2f}'.format(time()-start_time))
    print('average time per 1 news: {:.4f}'.format((time()-start_time) / len(articles)))

    if len(save_list)==0:
        print('pass')
        pass
    else:
        with open(os.path.join(target_path, file_name), 'w', encoding='utf-8') as f:
            json.dump(save_list, f, indent='\t', ensure_ascii=False)



if __name__ == '__main__':
    
    num_processor = 27

    preprocessor = LM_Preprocessor(is_news=True)

    pool = multiprocessing.Pool(processes=num_processor)
    m = multiprocessing.Manager()
    q = m.Queue()

    print('start!')
    #src_file_list = os.listdir(src_dir_path)
    #src_file_list.sort()
    #print(src_file_list)
    src_file_list = ['file_name.json','file_name2.json','file_name2.json','file_name2.json']

    for file_name in src_file_list:
        process_file(file_name)