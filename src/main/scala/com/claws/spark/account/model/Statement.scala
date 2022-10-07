package com.claws.spark.account.model

import java.util.Date


case class Statement(date : String,
                     transaction_detail : String,
                     ref_number : String,
                     value_date : String,
                     withdrawal : Double,
                     deposit : Double,
                     closing_balance : Double)
