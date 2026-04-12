"""
This script is for generating data for a fintech company. They are performing an A/B test on their
redesigned onboarding flow to test whether shorter onboarding leads to higher activation and higher paid conversion.
It contains the following tables:
1. users: one row per user
2. experiment_assignments: which variant each user was assigned
3. events: user-level event stream (funnel + engagement)
4. subscriptions: subscription/revenue outcomes per user

The A/B Test is:
Control (A): existing onboarding
Treatment (B): redesigned onboarding, which is shorter
Hypothesis: shorter onboarding leads to higher activation leads to higher paid conversion

I added some data quality issues into the data to ensure it is realistic:
- Around 2% of the values in device and acquisition_channel are null
- Around 1% of the event rows are duplicate
- Small sample ratio mismatch (52:48 split instead of 50:50)
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import hashlib
import os
import random

seed = 12
np.random.seed(seed)
random.seed(seed)

n_users = 10000

# 60 day experiment window
experiment_start = datetime(2024, 1, 15)
experiment_end = datetime(2024, 3, 15)

country_list = ["Germany", "Austria", "Switzerland", "Netherlands", "France"]
country_dist = [0.50, 0.15, 0.10, 0.15, 0.10]

device_list = ["mobile", "desktop", "tablet"]
device_dist = [0.60, 0.32, 0.08]

channels_list = ["organic", "paid_search", "referral", "social", "email"]
channels_dist = [0.30, 0.25, 0.20, 0.15, 0.10]

plans = ["free", "basic", "premium"]


def assign_variant(user_id: int) -> str:
    """
    Deterministic variant assignment via hashing, the data has a 52:48 split instead of 50:50.
    This slight ratio mismatch (SRM) is introduced by biasing the threshold (<52 instead of <50).
    """
    hash_val = int(hashlib.md5(f"exp_onboarding_v1_{user_id}".encode()).hexdigest(), 16)
    return "control" if (hash_val % 100) < 52 else "treatment"


def random_date(start: datetime, end: datetime) -> datetime:
    diff = end - start
    return start + timedelta(seconds=random.randint(0, int(diff.total_seconds())))


# 1. Users
print("Generating users")

user_ids = list(range(10001, 10001 + n_users))
signup_dates = [random_date(experiment_start, experiment_end) for _ in range(n_users)]
countries = np.random.choice(country_list, size=n_users, p=country_dist)

# devices and channels are cast to object type so null values (None) can be added to them
devices = np.random.choice(device_list, size=n_users, p=device_dist).astype(object)
channels = np.random.choice(channels_list, size=n_users, p=channels_dist).astype(object)

# adding 2% null values in devices and channels
null_devices_idx  = np.random.choice(n_users, size=int(n_users * 0.02), replace=False)
null_channels_idx = np.random.choice(n_users, size=int(n_users * 0.02), replace=False)
devices[null_devices_idx] = None
channels[null_channels_idx] = None

ages = np.random.randint(18, 65, size=n_users)

users_df = pd.DataFrame({
    "user_id": user_ids,
    "signup_date": signup_dates,
    "country": countries,
    "device": devices,
    "acquisition_channel": channels,
    "age": ages,
})

users_df["signup_date"] = pd.to_datetime(users_df["signup_date"])
print(f"Users: {len(users_df):,}")


# 2. Experiment Assignments
print("\nGenerating experiment assignments")

variants = [assign_variant(uid) for uid in user_ids]

exp_df = pd.DataFrame({
    "user_id": user_ids,
    "experiment_name": "onboarding_flow_v2",
    "variant": variants,
    "assigned_at": signup_dates,
})

control_n = (exp_df["variant"] == "control").sum()
treatment_n = (exp_df["variant"] == "treatment").sum()
print(f"Control: {control_n:,} | Treatment: {treatment_n:,} "
      f"(split: {control_n/n_users:.1%}/{treatment_n/n_users:.1%})")


# 3. Events
"""
Funnel steps in order: signup, email_verified, profile_completed, kyc_submitted, 
kyc_approved, payment_method_added, first_transaction, feature_engaged

Additional events (non-funnel):
- notification_clicked: engagement signal (CTR source)
- notification_sent: paired with clicked for CTR
"""

print("\nGenerating events")

funnel_steps = [
    "email_verified",
    "profile_completed",
    "kyc_submitted",
    "kyc_approved",
    "payment_method_added",
    "first_transaction",
    "feature_engaged",
]

# Control group
# Baseline completion probabilities per step = P(completing this step/completed previous step)
baseline_conversion_rates = {
    "email_verified": 0.85,
    "profile_completed": 0.75,
    "kyc_submitted": 0.65,
    "kyc_approved": 0.85,
    "payment_method_added": 0.70,
    "first_transaction": 0.65,
    "feature_engaged": 0.55,
}

# Treatment group
# How much higher treatment group probability is at each step
lifts = {
    "email_verified": 0.06,
    "profile_completed": 0.08,
    "kyc_submitted": 0.07,
    "kyc_approved": 0.02, # KYC approval is a bank's decision, not as affected by shorter onboarding
    "payment_method_added": 0.06,
    "first_transaction": 0.05,
    "feature_engaged": 0.04,
}

events_rows = []

for _, user in users_df.iterrows():
    uid = user["user_id"]
    variant = exp_df.loc[exp_df["user_id"] == uid, "variant"].values[0]
    t = user["signup_date"]

    # signup event always exists
    events_rows.append({
        "event_id": f"{uid}_signup",
        "user_id": uid,
        "event_name": "signup",
        "event_ts": t,
        "variant": variant,
    })

    # notifications sent to ~70% of users
    if np.random.rand() < 0.70:
        n_notifs = np.random.randint(1, 5)
        for i in range(n_notifs):
            t_notif = t + timedelta(hours=random.randint(1, 48))
            events_rows.append({
                "event_id": f"{uid}_notif_sent_{i}",
                "user_id": uid,
                "event_name": "notification_sent",
                "event_ts": t_notif,
                "variant": variant,
            })
            ctr_prob = 0.28 if variant == "treatment" else 0.22
            if np.random.rand() < ctr_prob:
                events_rows.append({
                    "event_id": f"{uid}_notif_click_{i}",
                    "user_id": uid,
                    "event_name": "notification_clicked",
                    "event_ts": t_notif + timedelta(minutes=random.randint(1, 60)),
                    "variant": variant,
                })

    # funnel steps
    prev_completed = True
    step_time = t + timedelta(minutes=random.randint(5, 30))

    for step in funnel_steps:
        if not prev_completed:
            break
        base_prob = baseline_conversion_rates[step]
        lift = lifts[step] if variant == "treatment" else 0.0
        prob = min(base_prob + lift, 0.99)

        if np.random.rand() < prob:
            step_time = step_time + timedelta(hours=random.randint(1, 24))
            events_rows.append({
                "event_id": f"{uid}_{step}",
                "user_id": uid,
                "event_name": step,
                "event_ts": step_time,
                "variant": variant,
            })
        else:
            prev_completed = False

events_df = pd.DataFrame(events_rows)
events_df["event_ts"] = pd.to_datetime(events_df["event_ts"])

# adding around 1% duplicate rows
n_dupes = int(len(events_df) * 0.01)
dupe_rows = events_df.sample(n=n_dupes, random_state=seed)
events_df = pd.concat([events_df, dupe_rows], ignore_index=True)
events_df = events_df.sample(frac=1, random_state=seed).reset_index(drop=True)

print(f"Events: {len(events_df):,} rows (including 1% duplicates)")


# 4. Subscriptions
"""
Subscription outcomes for users who completed first_transaction step. 
Churn is higher in first 30 days and then declines, treatment users have
slightly lower churn (better onboarding leads to better retention)
"""
print("\nGenerating subscriptions")

# Users who reached first_transaction
converted_users = (
    events_df[events_df["event_name"] == "first_transaction"]["user_id"]
    .drop_duplicates()
    .tolist()
)

plan_dist = [0.50, 0.35, 0.15]   # free, basic, premium
plan_mrr = {"free": 0, "basic": 9.99, "premium": 24.99}

sub_rows = []

for uid in converted_users:
    variant = exp_df.loc[exp_df["user_id"] == uid, "variant"].values[0]
    signup_date = users_df.loc[users_df["user_id"] == uid, "signup_date"].values[0]
    signup_date = pd.Timestamp(signup_date)

    plan = np.random.choice(plans, p=plan_dist)
    sub_start = signup_date + timedelta(days=random.randint(1, 5))
    mrr = plan_mrr[plan]

    # Churn probability: free users churn more, treatment churns less
    base_churn = {"free": 0.55, "basic": 0.30, "premium": 0.15}[plan]
    churn_prob = base_churn - (0.04 if variant == "treatment" else 0)

    churned = np.random.rand() < churn_prob
    churn_date = None
    if churned:
        # Early churn heavy in first 30 days
        if np.random.rand() < 0.60:
            days_to_churn = random.randint(3, 30)
        else:
            days_to_churn = random.randint(31, 90)
        churn_date = sub_start + timedelta(days=days_to_churn)

    sub_rows.append({
        "user_id": uid,
        "variant": variant,
        "plan": plan,
        "mrr": mrr,
        "sub_start_date": sub_start,
        "churned": churned,
        "churn_date": churn_date,
    })

subs_df = pd.DataFrame(sub_rows)
subs_df["sub_start_date"] = pd.to_datetime(subs_df["sub_start_date"])
subs_df["churn_date"] = pd.to_datetime(subs_df["churn_date"])

print(f"Subscriptions: {len(subs_df):,}")


# Save files
output_dir = "../data/input_data"
os.makedirs(output_dir, exist_ok=True)

users_df.to_csv(f"{output_dir}/users.csv", index=False)
exp_df.to_csv(f"{output_dir}/experiment_assignments.csv", index=False)
events_df.to_csv(f"{output_dir}/events.csv", index=False)
subs_df.to_csv(f"{output_dir}/subscriptions.csv", index=False)

# Sanity checks
print("\n\n--Sanity Checks--")

print("\nExperiment split:")
print(exp_df["variant"].value_counts(normalize=True).round(3))

print("\nFunnel completion rates by variant:")
funnel_events = events_df[events_df["event_name"].isin(funnel_steps + ["signup"])]
funnel_pivot  = (
    funnel_events.drop_duplicates(["user_id", "event_name"])
    .groupby(["variant", "event_name"])["user_id"]
    .count()
    .unstack("event_name")
    .fillna(0)
)
step_order = ["signup"] + funnel_steps
existing_steps = [s for s in step_order if s in funnel_pivot.columns]
funnel_pivot = funnel_pivot[existing_steps]
print(funnel_pivot.to_string())

print("\nSubscription plan distribution by variant:")
print(subs_df.groupby(["variant", "plan"]).size().unstack().fillna(0).astype(int))

print("\nChurn rate by variant and plan:")
churn_rates = subs_df.groupby(["variant", "plan"])["churned"].mean().unstack().round(3)
print(churn_rates)

print("\nNull counts in users:")
print(users_df.isnull().sum())

print("\nDuplicate event_id count:")
print(f"{events_df.duplicated('event_id').sum():,} duplicate event_ids")
