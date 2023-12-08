digits = [1]


end = len(digits) - 1
while end >= 0:
    if digits[end] < 9:
        digits[end] = digits[end] + 1
        break
    else:
        digits[end] = 0
        end -= 1
if end == -1:
    digits.insert(0,1)
print(digits)