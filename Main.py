__author__ = 'phlai'

import Crawler

if __name__ == "__main__":
    print("Go Test")

    #  Collect Index
    Crawler.get_all_indices_pay_data('IndexPayData')
    Crawler.get_all_indices_data('IndexData')


    #  Collect All Data
    Crawler.collect_main(['tse_price', 'tse_insti3', 'tse_mb', 'otc_price', 'otc_insti3', 'otc_mb'])
    # Crawler.collect_main(['tse_price', 'tse_mb'])
    # Crawler.collect_main(['otc_insti3'])
