let file_name;

function upload() {
  let node = document.getElementById("loading");
  if (node.hasChildNodes()) {
    node.removeChild(node.firstChild);
  }

  node = document.createElement("P")
  node.appendChild(document.createTextNode("処理を開始します。そのままお待ちください。"))
  document.getElementById("loading").appendChild(node)

  let formdata = new FormData(document.getElementById('upload'));
  if (formdata.get("file").size <= 0) {
    node = document.getElementById("loading");
    if (node.hasChildNodes()) {
      node.removeChild(node.firstChild);
      node = document.createElement("P")
      node.appendChild(document.createTextNode("適切なファイルを選択してください。"))
      document.getElementById("loading").appendChild(node)
    }
  }

  let xhttpreq = new XMLHttpRequest();
  xhttpreq.onreadystatechange = function() {
    if (xhttpreq.readyState == 4 && xhttpreq.status == 200) {
      node = document.getElementById("loading");
      if (node.hasChildNodes()) {
        node.removeChild(node.firstChild);
        node = document.createElement("P")
        node.appendChild(document.createTextNode("処理中です。数分お待ちください。"))
        document.getElementById("loading").appendChild(node)
      }

      file_name = JSON.parse(xhttpreq.responseText).file_name;
      let id = setInterval(function() {
        ready()
        if(!document.getElementById("downloadBtn").disabled){　
          clearInterval(id);
        }
      }, 10000);
    }
  };
  xhttpreq.open("POST", "/upload")
  xhttpreq.send(formdata);
}

function ready() {
  let xhttpreq = new XMLHttpRequest();
  xhttpreq.onreadystatechange = function() {
    if (xhttpreq.readyState == 4 && xhttpreq.status == 200) {
      document.getElementById("downloadBtn").disabled = false;

      let node = document.getElementById("loading");
      if (node.hasChildNodes()) {
        node.removeChild(node.firstChild);
        node = document.createElement("P")
        node.appendChild(document.createTextNode("処理が完了しました。"));
        document.getElementById("loading").appendChild(node)
      }
    }
  };
  xhttpreq.open("GET", `/ready/${file_name}`)
  xhttpreq.send();
}

function download() {
  let xhttpreq = new XMLHttpRequest();
  xhttpreq.onreadystatechange = function() {
    if (xhttpreq.readyState == 4 && xhttpreq.status == 200) {
      blob = new Blob([xhttpreq.response], {type: "text/csv"})
      file_name = getFileName(xhttpreq.getResponseHeader("Content-Disposition"))
      
      if (window.navigator.msSaveBlob) { 
        window.navigator.msSaveOrOpenBlob(blob, file_name); 
      } else {
        var downLoadLink = document.createElement("a");
        downLoadLink.download = file_name;
        downLoadLink.href = URL.createObjectURL(blob);
        downLoadLink.dataset.downloadurl = ["text/csv", downLoadLink.download, downLoadLink.href].join(":");
        downLoadLink.click();
      }
    }
  };
  xhttpreq.open("GET", `/download/${file_name}`)
  xhttpreq.send();
}

function getFileName(disposition) {
  const filenameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/; 
  const matches = filenameRegex.exec(disposition);
  if (matches != null && matches[1]) {
    const fileName = matches[1].replace(/['"]/g, '');
    return decodeURI(fileName) 
  } else {
    return null
  }
}