# ============= FUNCTION 2: PROCESS WITH OPENAI =============
def process_store_hours(df):
    print("\n🤖 Processing with OpenAI vision API...")
    
    recommendations, reasons, summary_reasons = [], [], []
    deactivation_reason_id, is_temp_deactivation = [], []
    confidence_scores = []
    
    bulk_hours = {day: {"start": [], "end": []} for day in [
        "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"
    ]}
    
    # Additional quality check phrases for hour changes
    hour_quality_issues = [
        "glare", "reflection", "reflective", "glaring", 
        "distance", "background", "far away", "far from",
        "difficult to read", "hard to read", "hard to make out",
        "small text", "tiny", "difficult to see clearly",
        "behind glass", "through window"
    ]
    
    for i, row in tqdm(df.iterrows(), total=len(df)):
        image_url = row.get("IMAGE_URL")
        store_hours = str(row.get("STORE_HOURS", ""))

        if not image_url or not store_hours:
            recommendations.append("No change")
            reasons.append("Missing image or hours")
            summary_reasons.append("Missing image or hours")
            deactivation_reason_id.append("")
            is_temp_deactivation.append(False)
            confidence_scores.append(0.0)
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
            continue

        prompt = f"""
You are reviewing a Dasher photo of a store entrance. CRITICAL: Check for closure signs FIRST before trying to read store hours.

PRIORITY ORDER (check in this order):
1) Is there a PERMANENT closure sign? (e.g., "permanently closing", "closed permanently", "thank you for your support")
2) Is there a TEMPORARY closure sign? (e.g., "POWER OUT", "closed due to weather", "system down", "maintenance", "closed today")
3) Are the posted store hours clearly visible AND readable?

Choose ONE recommendation:
- **Permanently Close Store** - ONLY if you see clear permanent closure signage
- **Temporarily Close For Day** - If you see ANY temporary closure sign (power out, maintenance, weather, system issues, etc.) - DO NOT try to read hours if this applies
- **Change Store Hours** - ONLY if there are NO closure signs AND the hours are clearly readable (clarity > 0.9) AND not affected by glare/reflection/distance
- **No Change** - If hours are blurry/unreadable OR match DoorDash hours OR affected by glare/reflection

IMPORTANT: 
- If you see a "POWER OUT", "CLOSED", or any temporary closure sign, recommend "Temporarily Close For Day" and DO NOT attempt to extract store hours from the background.
- If the hours are visible but have ANY of these issues, recommend "No Change": glare, reflection, far away in background, behind glass with reflections, small/hard to read text
- Be honest if you're having ANY difficulty reading the exact times due to image quality

Current DoorDash hours: {store_hours}

If recommending changed hours, list the full weekly schedule clearly (e.g. Monday: 08:00 - 22:00).

Provide this line at the end:
Clarity score: X
Where X is a number between 0.0 and 1.0 for how clearly ANY signage is visible. If there's a closure sign, rate that sign's clarity. For hours, consider if glare/reflection/distance affects readability.
"""
        prompt += "\nAssume store closing times like '10:00' or '12:00' without AM/PM are in the evening (PM)."

        try:
            response = openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": image_url}}
                    ]}
                ],
                max_tokens=1000
            )

            result = response.choices[0].message.content.strip()
            reason = result
            lower = result.lower()

            posted = extract_hours(result)
            parse_coverage = confidence_from_hours(posted)
            clarity = extract_clarity_score(result)

            # If clarity is too low, skip
            if clarity < 0.9:
                recommendations.append("No change")
                reasons.append("Clarity too low (<0.9), skipping recommendation")
                summary_reasons.append("Low clarity image")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(combine_confidence(parse_coverage, clarity))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # Check for uncertainty
            if any(p in lower for p in uncertain_phrases):
                recommendations.append("No change")
                reasons.append("Model expressed uncertainty despite clarity threshold")
                summary_reasons.append("Image unreadable or GPT uncertain")
                deactivation_reason_id.append("")
                is_temp_deactivation.append(False)
                confidence_scores.append(combine_confidence(parse_coverage, clarity))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # NEW: Check for image quality issues that affect hour readability
            has_quality_issues = any(phrase in lower for phrase in hour_quality_issues)

            # PRIORITY 1: Check for PERMANENT closure
            if "permanently close" in lower and is_permanent_closure(result):
                recommendations.append("Permanently Close Store")
                reasons.append(reason)
                summary_reasons.append("Permanent closure detected")
                deactivation_reason_id.append("23")
                is_temp_deactivation.append(False)
                confidence_scores.append(0.95)
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 2: Check for TEMPORARY closure (highest priority after permanent)
            if "temporarily close" in lower or "special hour" in lower:
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append(categorize_closure(lower))
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.8, combine_confidence(parse_coverage, clarity)))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # Additional temp closure checks
            if (any(phrase in lower for phrase in [
                "closed for the day", "closed today", "closed due to", "store is closed",
                "power out", "no power", "maintenance", "system down"
            ]) and "permanently" not in lower):
                recommendations.append("Temporarily Close For Day")
                reasons.append(reason)
                summary_reasons.append(categorize_closure(lower))
                deactivation_reason_id.append("67")
                is_temp_deactivation.append(True)
                confidence_scores.append(max(0.8, combine_confidence(parse_coverage, clarity)))
                for day in bulk_hours:
                    bulk_hours[day]["start"].append("")
                    bulk_hours[day]["end"].append("")
                continue

            # PRIORITY 3: Only NOW check for hour changes (if no closures detected)
            if "recommend" in lower and "change store hour" in lower:
                # NEW: Block hour changes if quality issues detected
                if has_quality_issues:
                    recommendations.append("No change")
                    reasons.append("Image quality issues detected (glare/reflection/distance) - skipping hour change")
                    summary_reasons.append("Image quality issues for hour extraction")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                
                if any(p in lower for p in uncertain_phrases):
                    recommendations.append("No change")
                    reasons.append("GPT suggested change but was uncertain")
                    summary_reasons.append("GPT suggested change but was uncertain")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue

                listed = extract_hours(store_hours)

                if len(posted) in [1, 2]:
                    starts = set(v["start"] for v in posted.values() if v.get("start"))
                    ends = set(v["end"] for v in posted.values() if v.get("end"))
                    if len(starts) == 1 and len(ends) == 1:
                        same_start = list(starts)[0]
                        same_end = list(ends)[0]
                        posted = {day: {"start": same_start, "end": same_end} for day in bulk_hours}
                        parse_coverage = confidence_from_hours(posted)

                if len(posted) < 5:
                    recommendations.append("No change")
                    reasons.append("Too few days extracted to safely change hours (>=5 required)")
                    summary_reasons.append("Too few days extracted to safely change hours")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue

                minor_diff = True
                for day in posted:
                    if day in listed:
                        p_start, p_end = posted[day]["start"], posted[day]["end"]
                        l_start, l_end = listed[day]["start"], listed[day]["end"]
                        if time_diff_min(p_start, l_start) > 5 or time_diff_min(p_end, l_end) > 5:
                            minor_diff = False
                            break
                    else:
                        minor_diff = False
                        break

                if minor_diff:
                    recommendations.append("No change")
                    reasons.append(reason)
                    summary_reasons.append("Only minor time difference")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        bulk_hours[day]["start"].append("")
                        bulk_hours[day]["end"].append("")
                    continue
                else:
                    recommendations.append("Change Store Hours")
                    reasons.append(reason)
                    summary_reasons.append("Posted hours differ from DoorDash hours")
                    deactivation_reason_id.append("")
                    is_temp_deactivation.append(False)
                    confidence_scores.append(combine_confidence(parse_coverage, clarity))
                    for day in bulk_hours:
                        raw_start = posted.get(day, {}).get("start", "")
                        raw_end = posted.get(day, {}).get("end", "")
                        bulk_hours[day]["start"].append(normalize_time(raw_start))
                        bulk_hours[day]["end"].append(normalize_time(raw_end))
                    continue

            recommendations.append("No change")
            reasons.append(reason)
            summary_reasons.append("No change required")
            deactivation_reason_id.append("")
            is_temp_deactivation.append(False)
            confidence_scores.append(combine_confidence(parse_coverage, clarity))
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")

        except Exception as e:
            recommendations.append("Error")
            reasons.append(str(e))
            summary_reasons.append("GPT error")
            deactivation_reason_id.append("")
            is_temp_deactivation.append(False)
            confidence_scores.append(0.0)
            for day in bulk_hours:
                bulk_hours[day]["start"].append("")
                bulk_hours[day]["end"].append("")
    
    df["RECOMMENDATION"] = recommendations
    df["REASON"] = reasons
    df["SUMMARY_REASON"] = summary_reasons
    df["deactivation_reason_id"] = deactivation_reason_id
    df["is_temp_deactivation"] = is_temp_deactivation
    df["CONFIDENCE_SCORE"] = confidence_scores

    for day in bulk_hours:
        df[f"start_time_{day}"] = bulk_hours[day]["start"]
        df[f"end_time_{day}"] = bulk_hours[day]["end"]
    
    return df
