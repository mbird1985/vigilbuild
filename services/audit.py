@app.route('/audit')
@login_required
def audit():
    if not current_user.is_admin():
        flash('Admin access required.')
        return redirect(url_for('serve_search'))

    if not es.indices.exists(index="audit"):
        es.indices.create(index="audit")

    response = es.search(index="audit", query={"match_all": {}}, size=1000, sort={"timestamp": {"order": "desc"}})
    audit_entries = [{
        "action": hit["_source"]["action"],
        "user_id": hit["_source"]["user_id"],
        "document_id": hit["_source"].get("document_id"),
        "document_title": hit["_source"].get("document_title"),
        "details": hit["_source"].get("details", {}),
        "timestamp": hit["_source"]["timestamp"]
    } for hit in response["hits"]["hits"]]
    return render_template('audit.html', audit_entries=audit_entries)