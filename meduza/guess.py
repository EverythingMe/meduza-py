# -*- coding: utf-8 -*-
__author__ = 'dvirsky'

import random
import sys
import time

def guessit():

    X = 1000000000

    print "בחר מספר בין אפס ל", X

    num = int(sys.stdin.readline().strip())
    if num > X or num < 0:
        print "נסה מספר אחר"


    top = X
    bottom = 0
    i = 1
    while True:

        V = bottom + int((top-bottom)/2)
        print i, ":", V
        i += 1

        try:

            if V == num:
                print "*********** צדקת!!! ***************"
                break
            else:
                if V > num:

                    print "נסה מספר יותר נמוך"
                    top = V
                else:
                    bottom = V
                    print "נסה מספר גבוה יותר"

        except Exception:

            continue


if __name__ == '__main__':

    guessit()

