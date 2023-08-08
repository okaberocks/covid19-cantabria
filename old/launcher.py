import warnings
from datetime import datetime

today = datetime.now()
print("Today's date:", today)
# warnings.filterwarnings("ignore")

import historical
import current_situation
import old.totals as totals
import age_sex
import accumulated
import daily
import sma
import municipal
# import mult_rate
# import vaccine
import rho
import estimation_arima
import estimation_loess
import estimation_redneu

print('Finished')