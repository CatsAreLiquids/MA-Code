from Agent import init_agent
import pandas as pd

agent = init_agent()


def test_gen():
    plans = []
    query = "What is Copycat's race?"
    for i in range(50):
        try:
            plan = agent.invoke({agent.invoke({'input': query})})
            plans.append(plan['output'])
        except:
            i -= 1

    df = pd.DataFrame(plans, columns=['plans'])
    df.to_csv("test_output.csv")


if __name__ == "__main__":
    test_gen()
