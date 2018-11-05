common = '/html/body/div[2]/div[3]/div[2]/div[5]/div/div/span/div/div/div[2]/div/div/div[1]/div[2]/div/table/tbody/'
mp3 = '/td[2]/div/img'
time = '/td[4]/div'

tr = 'tr'

def loop_through_xpaths(common, ending, num):
    tr = 'tr'
    for i in range(num):
        yield (common + tr + '[' + str(i+1)+ ']' + ending)


if __name__ == '__main__':
    xpath = loop_through_xpaths(common, time, 5)
    print(next(xpath))
    print(next(xpath))
    for i in range(5):
        print(next(xpath))
