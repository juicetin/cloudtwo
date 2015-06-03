import sys

nums = []
for string in sys.argv:
    nums.append(string)
nums = nums[1:]

print (nums) 
#nums = [int(i) for i in nums]
#print (sum(nums))
