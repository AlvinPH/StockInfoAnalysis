__author__ = 'phlai'

import Crawler

if __name__ == "__main__":
    print("Go Test")

    #  Collect Index
    # Crawler.get_all_indices_pay_data('IndexPayData')
    # Crawler.get_all_indices_data('IndexData')


    #  Collect All Data
    # Crawler.collect_main(['tse_price', 'tse_insti3', 'tse_mb'])
    Crawler.collect_main(['tse_mb'])
    Crawler.collect_main(['otc_price', 'otc_insti3', 'otc_mb'])
    # Collect Actions

    # collect = Crawler.UpdateActions(['StockInfo_otc', 'StockInfo'])
    # collect.check_all_data()
