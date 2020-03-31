#!/bin/awk

BEGIN {
    FS = ''", "|^"|"$''
}

NR > 1 {
  instrument_id = $1
  opening_balance = $7
  closing_balance = $8

  print opening_balance closing_balance
}

END {

}