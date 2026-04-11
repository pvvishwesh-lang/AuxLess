import { useState, useEffect } from 'react';
import { doc, onSnapshot, collection } from 'firebase/firestore';
import { db } from '../config/firebase';

// Listens to sessions/{roomId} + members
export function useRoom(roomId) {
  const [room,    setRoom]    = useState(null);
  const [members, setMembers] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!roomId) return;

    // Listen to session doc
    const unsubRoom = onSnapshot(
      doc(db, 'sessions', roomId),
      (snap) => {
        if (snap.exists()) {
          const data = snap.data();
          setRoom(data);
          setMembers(data.users || []);
        }
        setLoading(false);
      }
    );

    return () => unsubRoom();
  }, [roomId]);

  return { room, members, loading };
}