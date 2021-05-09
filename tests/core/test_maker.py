# # print("calling method")
# a = maker.make(1, 2)
#
#
# class Test:
#
#     @job
#     @staticmethod
#     def static_before(a, b):
#         return a + b
#
#     @staticmethod
#     @job
#     def static_after(a, b):
#         return a + b
#
#     @job
#     @classmethod
#     def class_before(cls, a, b):
#         return a + b
#
#     @classmethod
#     @job
#     def class_after(cls, a, b):
#         return a + b
#
#
# # print("calling static after")
# a = Test.static_after(3, 4)
# # print("calling static before")
# a = Test.static_before(3, 4)
# #
# # print("calling class after")
# a = Test.class_after(3, 4)
# # print("calling class before")
# a = Test.class_before(3, 4)
#
# output = run_locally(a)
# print(list(output.values())[0])
#
